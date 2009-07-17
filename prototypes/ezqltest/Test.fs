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
    eval Map.empty (Parser.Expr Lexer.token (Lexing.from_string expr))
  with
    | e -> failwithf "Error parsing '%s'" expr

let matchFact (fact:diff) (ocurred:diff) =
  match fact, ocurred with
  | HidKeyDiff (key, true, _), HidKeyDiff (key', true, _) when key = key' -> true
  | _ when fact = ocurred -> true
  | _ -> false

let AddedOrExpired factMaker eventTimestamp expr factTimestamp =
  let value = tryEval expr
  match value with
  | VRecord fields -> 
      let fields' = Map.fold_left (fun acc k v -> Map.add k v acc) Map.empty fields
      let ev = VRecord (fields'.Add(VString "timestamp", VInt eventTimestamp))
      (factTimestamp, factMaker ev)
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
  (timestamp, (fact.Diff (HidKeyDiff (key, true, []))))


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
  let sink = Operator.Build("__sink" + op.Uid, (Priority.add op.Priority (Priority.of_list [0; 0; 0; 1])),
                            (fun (op, changes) -> action(changes); Nothing),
                            [op], op.Context)
  // Force the sink to verify the last assert.                            
  Scheduler.scheduleOffset 1000000 (List.of_seq [sink, 0, id], [diff.Added VNull])
  sink
    
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
        let delayed = ref None
        addSinkTo oper
          (function
            | changes::_ ->
                let now = Engine.now()
                
                // We only assert changes for timestamp n after n
                match !delayed with
                | Some (time, prevChanges, _) when time = now -> delayed := Some (time, mergeChanges prevChanges changes, oper.Value)
                | None -> delayed := Some (now, changes, oper.Value)
                | Some (pTime, pChanges, value) ->
                    // Ignore HidKeyDiffs
                    let pChanges' = List.filter (fun diff -> match diff with
                                                             | HidKeyDiff (_, false, _) -> false
                                                             | _ -> true)
                                                pChanges
                    let facts = Map.tryFind pTime (!timeToFact)
                    match facts with
                    | Some facts' ->
                        if not (facts'.Length >= pChanges'.Length && facts'.Length <= pChanges.Length)
                          then failwithf "In %s, at %A: the number of predicted changes is different\n from the actual number of changes:\n - %A\n - %A\n"
                                         entity pTime.TotalSeconds pChanges' facts'
                        for fact' in facts' do
                          match fact' with
                          | Diff fact'' -> if not (List.exists (matchFact fact'') pChanges)
                                             then failwithf "In %s, at %A: the diffs differ!\n\t Happened: %A\n\t Expected: %A\n"
                                                            entity pTime.TotalSeconds pChanges facts'
                          | ValueAtKey (k, v) ->
                              match value with
                              | VDict dict ->
                                  if Map.contains k dict
                                    then let v' = dict.[k]
                                         if v <> v' then failwithf "In %s, at %A: the values for key %A differ!\n\t Current: %O\n\t Expected: %O\n"
                                                                   entity pTime.TotalSeconds k dict.[k] v
                                    else failwithf "In %s, at %A: couldn't find the key %A in the dictionary!\n"
                                                   entity pTime.TotalSeconds k
                              | _ -> failwithf "The entity '%s' is not a dictionary! Is is %A" entity value
                          | Value v -> printfn "ola"
                        timeToFact := Map.remove pTime !timeToFact
                    | _ -> if (not pChanges'.IsEmpty)
                             then failwithf "  In %s, at %A: unpredicted event happened:\n\t %A\n"
                                            entity pTime.TotalSeconds pChanges'
                    delayed := Some (now, changes, oper.Value)
            | [] -> failwithf "  In %s, at %A: entity is spreading empty changes." entity (Engine.now().TotalSeconds)) |> ignore
    | _ -> failwithf "\n\n\nAssertThat: Couldn't find symbol '%s'\n\n\n" entity

let init code inputs ticksUpTo =
  let env = Engine.compile code
  for inputStream, events in inputs do
    match Map.tryFind inputStream env with
    | Some op -> CSVAdapter.FromString(op, events) |> ignore
    | _ -> failwithf "Cannot find the input stream '%s'" inputStream
  
  // Tell "ticks" to stop at ticksUpTo
  env.["ticks"].Eval(env.["ticks"], [[diff.Expired (VInt ticksUpTo)]]) |> ignore
  
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

type TestCaseAttribute(srcFile:string, ticksUpTo:int) =
    inherit Attribute()
    new (srcFile) = TestCaseAttribute(srcFile, -1)
    member self.TicksUpTo = ticksUpTo
    member self.CreateTest() =
        let code, inputs = parseTestFile (@"test/" + srcFile)
        init code inputs ticksUpTo


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
