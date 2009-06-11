open System
open System.Reflection
open System.Collections.Generic
open System.IO
open Adapters
open Types
open Eval
open Extensions.DateTimeExtensions
open Oper

(* Test DSL *)

type fact =
  | Diff of diff
  | Value of value
  | ValueAtKey of value * value

type FactMap = Map<DateTime, fact list>

let At (timestamp:int) =
    DateTime.FromSeconds(timestamp)

let After time = DateTime.FromSeconds(time)

let tryEval expr =
  try
    eval Map.empty (Parser.expr Lexer.token (Lexing.from_string expr))
  with
    | e -> failwithf "Error parsing '%s'" expr

let AddedOrExpired factMaker eventTimestamp expr factTimestamp =
  let value = tryEval expr
  match value with
  | VRecord fields -> 
      let fields' = Map.fold_left (fun acc k v -> match k with
                                                  | VString k' -> Map.add k' !v acc
                                                  | _ -> failwith "Not a VString!")
                                  Map.empty fields
      (factTimestamp, factMaker (VEvent (Event (DateTime.FromSeconds(eventTimestamp), fields'))))
  | _ -> (factTimestamp, factMaker value)

let Added evTime expr factTime = AddedOrExpired (fact.Diff << diff.Added) evTime expr factTime
let Expired evTime expr factTime = AddedOrExpired (fact.Diff << diff.Expired) evTime expr factTime

//let ExpiredAll (timestamp:DateTime) = (timestamp, fact.AllExpired)

let Set expr (timestamp:DateTime) =
    Added (timestamp.TotalSeconds) expr timestamp

let Del eventTimestamp expr (timestamp:DateTime) =
    Expired eventTimestamp (sprintf "{ :value = %s }" expr) timestamp

let SetKeyRaw keyExpr value timestamp =
  let key = tryEval keyExpr
  (timestamp, ValueAtKey (key, value))

let SetKey keyExpr expr timestamp = SetKeyRaw keyExpr (tryEval expr) timestamp

let DelKey keyExpr (timestamp:DateTime) =
  let key = tryEval keyExpr
  (timestamp, (fact.Diff (RemovedKey key)))


let In entity (facts:(DateTime * fact) list) =
 (*   let interval, f = facts.Head
    let facts' = 
        match f with
 //       | AllExpired -> (List.map (fun (t:DateTime, f) -> 
 //                                     match f with
 //                                     | EventAdded ev -> (t.AddSeconds(float(interval.TotalSeconds)), EventExpired ev)
 //                                     | _ -> failwith "Unexpected fact")
 //                                 facts.Tail) @ facts.Tail
        | _ -> facts
        *)
    (entity, facts)


(* Operator support *)

let addSinkTo op action =
  Operator.Build("__sink" + op.Uid, (Priority.add op.Priority (Priority.of_list [0; 0; 0; 1])),
    (fun (op, changes) -> action(changes); None),
    [op])
    
(* A Test *)

let list2Map list merge =
  List.fold (fun acc (k, v) -> 
               let v' = if Map.contains k acc then merge v acc.[k] else [v]
               Map.add k v' acc)
            Map.empty list

type Test =
  { code:string; env:Map<string, Operator>; allFacts:List<FactMap ref> }
  member self.AssertThat((entity:string, facts)) =
    let timeToFact = ref (list2Map facts (fun a b -> a::b))
    self.allFacts.Add(timeToFact)
    let operOpt = Map.tryFind entity self.env
    match operOpt with
    | Some oper ->
        addSinkTo oper
          (function
            | changes::_ ->
                //printfn "changes = %A" changes
                let now = Engine.now()
                let facts = Map.tryFind now (!timeToFact)
                match facts with
                | Some facts' ->
                    if facts'.Length <> changes.Length
                      then failwithf "In %s, at %A: the number of predicted changes is different\n from the actual number of changes:\n - %A\n - %A\n"
                                     entity now.TotalSeconds changes facts'
                    for fact' in facts' do
                      match fact' with
                      | Diff fact'' -> if not (List.contains fact'' changes)
                                         then failwithf "In %s, at %A: the diffs differ!\n\t Happened: %A\n\t Expected: %A\n"
                                                        entity now.TotalSeconds changes facts'
                      | ValueAtKey (k, v) ->
                          match oper.Value with
                          | VDict dict ->
                              if Map.contains k !dict
                                then let v' = (!dict).[k]
                                     if v <> v' then failwithf "In %s, at %A: the values for key %A differ!\n\t Current: %O\n\t Expected: %O\n"
                                                               entity now.TotalSeconds k (!dict).[k] v
                                else failwithf "In %s, at %A: couldn't find the key %A in the dictionary!\n"
                                               entity now.TotalSeconds k
                          | _ -> failwithf "The entity '%s' is not a dictionary!" entity
                      | Value v -> printfn "ola"
                    timeToFact := Map.remove now !timeToFact
                | _ -> failwithf "  In %s, at %A: unpredicted event happened:\n\t %A\n"
                                 entity now.TotalSeconds changes
            | [] -> failwithf "  In %s, at %A: entity is spreading empty changes." entity (Engine.now().TotalSeconds)) |> ignore
    | _ -> failwithf "\n\n\nAssertThat: Couldn't find symbol '%s'\n\n\n" entity

let init code inputs =
  let env = Engine.compile code
  for inputStream, events in inputs do
    match Map.tryFind inputStream env with
    | Some op -> CSVAdapter.FromString(op, events) |> ignore
    | _ -> failwithf "Cannot find the input stream '%s'" inputStream
  { code = code; env = env; allFacts = List<FactMap ref>() }

let testsLeft test =
    Seq.to_list (Seq.concat [ for map in test.allFacts -> Map.to_list (!map) ])
    
(* TestCases *)

let parseTestFile fileName =
    let code, inputs = Array.fold (fun (c, i) (n:string) -> 
                                     if n.StartsWith("#!") 
                                         then (c, (n.Split([|' '|]).[1], "")::i)
                                         else if not (List.isEmpty i)
                                                  then let hd = i.Head
                                                       (c, (fst hd, (snd hd) + n + "\n")::i.Tail)
                                                  else (c + n + "\r\n", i))
                                ("", [])
                                (File.ReadAllLines(fileName))
    code, inputs

type TestCaseAttribute(srcFile:string) =
    inherit Attribute()
    member self.CreateTest() =
        //let code, inputs = parseTestFile (@"C:\streampy\prototypes\ezql\test\" + srcFile)
        let code, inputs = parseTestFile (@"test/" + srcFile)
        init code inputs


//printfn "%A" (Assembly.GetExecutingAssembly().Location)
let currentAssembly = Assembly.LoadFrom(Assembly.GetExecutingAssembly().Location)

let findTests () =
    let asm = currentAssembly
    [ for typ in asm.GetTypes() do
        if typ.IsClass then
            let typ' = asm.GetType(typ.FullName)
            yield! [ for m in typ'.GetMethods(BindingFlags.Static ||| BindingFlags.Public) do
                       if Array.exists (fun (a:obj) -> a :? TestCaseAttribute) (m.GetCustomAttributes(true))
                         then yield m ] ]

let findTest testName =
    List.find (fun (t:MethodInfo) -> t.Name = testName) (findTests ())

let runTests (testMethods:MethodInfo list) =
  for testMethod in testMethods do
    let attr = testMethod.GetCustomAttributes(true).[0] :?> TestCaseAttribute
    let test = attr.CreateTest()
    let testName = testMethod.Name
    printfn "- Testing %s\n" testName
  //  try
    testMethod.Invoke(null, [|box test|]) |> ignore
    Engine.mainLoop ()
    let left = testsLeft test
    if left.IsEmpty
      then printfn "\t\t\t\t\t\t\t\tPass"
      else printfn "| Tests left in test '%s'\n%A\n\n" testName left
   // with
   //   | SpreadException (op, _, inner) -> printfn "SpreadException at %s:\n %A\n%s\n\n" op.Uid inner inner.StackTrace
   //   | err -> printfn "Exception:\n %A\n\n" err

    Engine.reset ()
