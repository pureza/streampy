﻿#light

open System
open System.Reflection
open System.Collections.Generic
open System.IO
open Adapters
open Types
open Eval
open DateTimeExtensions

(* Test DSL *)

type fact =
    | EventAdded of Event
    | EventExpired of Event
    | AllExpired

type FactMap = Map<DateTime, fact>

let At (timestamp:int) =
    DateTime.FromSeconds(timestamp)

let After time = DateTime.FromSeconds(time)

let AddedOrExpired factMaker eventTimestamp expr factTimestamp =
  let record = eval Map.empty (Parser.expr Lexer.token (Lexing.from_string expr))
  match record with
  | VRecord fields -> 
      let fields' = Map.fold_left (fun acc k v -> match k with
                                                  | VString k' -> Map.add k' !v acc
                                                  | _ -> failwith "Not a VString!")
                                  Map.empty fields
      (factTimestamp, factMaker (Event (DateTime.FromSeconds(eventTimestamp), fields')))
  | _ -> failwithf "Not a record?"

let Added evTime expr factTime = AddedOrExpired EventAdded evTime expr factTime
let Expired evTime expr factTime = AddedOrExpired EventExpired evTime expr factTime

let ExpiredAll (timestamp:DateTime) = (timestamp, fact.AllExpired)

let Set expr (timestamp:DateTime) =
    Added (timestamp.TotalSeconds) (sprintf "{ :value = %s }" expr) timestamp

let Del eventTimestamp expr (timestamp:DateTime) =
    Expired eventTimestamp (sprintf "{ :value = %s }" expr) timestamp
    
let AddKey = Set

let DelKey expr (timestamp:DateTime) =
    Expired timestamp.TotalSeconds (sprintf "{ :value = %s }" expr) timestamp    

let In entity (facts:(DateTime * fact) list) =
    let interval, f = facts.Head
    let facts' = 
        match f with
        | AllExpired -> (List.map (fun (t:DateTime, f) -> 
                                      match f with
                                      | EventAdded ev -> (t.AddSeconds(float(interval.TotalSeconds)), EventExpired ev)
                                      | _ -> failwith "Unexpected fact")
                                  facts.Tail) @ facts.Tail
        | _ -> facts
    (entity, facts')

(*
let AssertThat(entity, (istream:IStream<IEvent>), (rstream:IStream<IEvent>), facts) =
  let queue = SortedList<DateTime, fact>(Map.of_list facts)
  let checkBuilder factType = 
      (fun ev ->
          let now = Engine.now ()
          while queue.Count > 0 && queue.Keys.[0] < now do
              printfn "[%s] Error: Predicted fact '%A' at %A didn't happen!" entity
                  queue.Values.[0] queue.Keys.[0].TotalSeconds
              queue.RemoveAt(0) |> ignore
          
          let fact = (factType ev)
          if queue.Count > 0 && queue.Keys.[0] = now && queue.[now] = fact
              then queue.Remove(now) |> ignore
              else printfn "[%s] Warning: Non-predicted fact '%A' occurred at %A" entity fact now.TotalSeconds)

  istream.OnAdd (checkBuilder EventAdded)
  rstream.OnAdd (checkBuilder EventExpired)
  queue
*)

(* Operator support *)

let addSinkTo op action =
  Operator.Build("__sink" + op.Uid, -1.0,
    (fun op changes -> action(changes); None),
    [op])
    
(* A Test *)

type Test =
  { code:string; env:Map<string, Operator>; allFacts:List<FactMap ref> }
  member self.AssertThat((entity:string, facts)) =
    let timeToFact = ref (Map.of_list facts)
    self.allFacts.Add(timeToFact)
    let entityOp = self.env.[entity]
    addSinkTo entityOp
      (fun changes ->
         let now = Engine.now()
         let fact = Map.tryfind now (!timeToFact)
         match fact with
         | Some fact' -> match changes.Head, fact' with
                         | [Added (VEvent ev1)], EventAdded ev2 ->
                             if ev1 = ev2
                               then timeToFact := Map.remove now !timeToFact
                               else printfn "At %A: the events differ!\n  Happened: %A\n  Expected: %A\n" now.TotalSeconds ev1 ev2
                         | _ -> printfn "Check this out when it happens!"
         | _ -> printfn "At %A: unpredicted event happened:\n  %A\n" now.TotalSeconds changes.Head) |> ignore

let init code inputs =
  let streams, env = Engine.compile code
  for inputStream, events in inputs do
    CSVAdapter.FromString(env.[inputStream], events) |> ignore
  { code = code; env = env; allFacts = List<FactMap ref>() }

let testsLeft test =
    Seq.to_list (Seq.concat [ for map in test.allFacts -> Map.to_list (!map) ])
    
(* TestCases *)

let parseTestFile fileName =
    let code, inputs = Array.fold_left (fun (c, i) (n:string) -> 
                                            if n.StartsWith("#!") 
                                                then (c, (n.Split([|' '|]).[1], "")::i)
                                                else if not (List.is_empty i)
                                                         then let hd = i.Head
                                                              (c, (fst hd, (snd hd) + n + "\n")::i.Tail)
                                                         else (c + n + "\r\n", i))
                                       ("", [])
                                       (File.ReadAllLines(fileName))
    code, inputs

type TestCaseAttribute(srcFile:string) =
    inherit Attribute()
    member self.CreateTest() =
        let code, inputs = parseTestFile (@"C:\streampy\prototypes\ezql\test\" + srcFile)
        init code inputs

let currentAssembly = Assembly.LoadFrom(Assembly.GetExecutingAssembly().GetName().Name + ".exe")

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
        try
            testMethod.Invoke(null, [|box test|]) |> ignore
        with
          | err -> printfn "%A" err
        Engine.mainLoop ()
        let left = testsLeft test
        if left.IsEmpty
            then printfn "\t\t\t\t\t\t\t\tPass"
            else printfn "| Tests left in test '%s'\n%A\n\n" testName left

        Engine.reset ()
        


(*
open DateTimeExtensions

(* Test DSL *)

type fact =
    | EventAdded of IEvent
    | EventExpired of IEvent
    | AllExpired

type testQueue = SortedList<DateTime, fact>

let At (timestamp:int) =
    DateTime.FromSeconds(timestamp)

let After time = DateTime.FromSeconds(time)

let Added eventTimestamp expr factTimestamp =
    let record = evalE Map.empty (EzqlParser.expr EzqlLexer.token (Lexing.from_string expr))
    match record with
    | VRecord fields -> (factTimestamp, 
                         EventAdded (Event(DateTime.FromSeconds(eventTimestamp), fields)))
    | _ -> failwithf "Not a record?"

let Expired eventTimestamp expr factTimestamp =
    let record = evalE Map.empty (EzqlParser.expr EzqlLexer.token (Lexing.from_string expr))
    match record with
    | VRecord fields -> (factTimestamp, 
                         EventExpired (Event(DateTime.FromSeconds(eventTimestamp), fields)))
    | _ -> failwithf "Not a record?"

let ExpiredAll (timestamp:DateTime) = (timestamp, fact.AllExpired)

let Set expr (timestamp:DateTime) =
    Added (timestamp.TotalSeconds) (sprintf "{ :value = %s }" expr) timestamp

let Del eventTimestamp expr (timestamp:DateTime) =
    Expired eventTimestamp (sprintf "{ :value = %s }" expr) timestamp
    
let AddKey = Set

let DelKey expr (timestamp:DateTime) =
    Expired timestamp.TotalSeconds (sprintf "{ :value = %s }" expr) timestamp    

let In entity (facts:(DateTime * fact) list) =
    let interval, f = facts.Head
    let facts' = 
        match f with
        | AllExpired -> (List.map (fun (t:DateTime, f) -> 
                                      match f with
                                      | EventAdded ev -> (t.AddSeconds(float(interval.TotalSeconds)), EventExpired ev)
                                      | _ -> failwith "Unexpected fact")
                                  facts.Tail) @ facts.Tail
        | _ -> facts
    (entity, facts')

let AssertThat(entity, (istream:IStream<IEvent>), (rstream:IStream<IEvent>), facts) =
    let queue = SortedList<DateTime, fact>(Map.of_list facts)
    let checkBuilder factType = 
        (fun ev ->
            let now = Engine.now ()
            while queue.Count > 0 && queue.Keys.[0] < now do
                printfn "[%s] Error: Predicted fact '%A' at %A didn't happen!" entity
                    queue.Values.[0] queue.Keys.[0].TotalSeconds
                queue.RemoveAt(0) |> ignore
            
            let fact = (factType ev)
            if queue.Count > 0 && queue.Keys.[0] = now && queue.[now] = fact
                then queue.Remove(now) |> ignore
                else printfn "[%s] Warning: Non-predicted fact '%A' occurred at %A" entity fact now.TotalSeconds)

    istream.OnAdd (checkBuilder EventAdded)
    rstream.OnAdd (checkBuilder EventExpired)
    queue

(* A Test *)

type Test =
    { code:string; env:Map<string, value>; queues:ICollection<testQueue> }
    member self.AssertThat((entity, facts)) =
        let entityExpr = (EzqlParser.expr EzqlLexer.token (Lexing.from_string entity))
        self.queues.Add(match (evalE self.env entityExpr) with
                        | VStream stream -> AssertThat(entity, stream.InsStream, stream.RemStream, facts)
                        | VContinuousValue cv -> 
                            let istream = cv.InsStream.Select (fun (t, v) -> Event.WithValue(t, v))
                            let rstream = cv.InsStream.Select (fun (t, v) -> Event.WithValue(t, v))
                            AssertThat(entity, istream, rstream, facts)
                      //  | VMap assoc -> AssertThat(entity, assoc.InsStream, assoc.RemStream, facts)
                        | _ -> failwith "entity is neither stream nor continuous value")
    

let init code inputs =
    let env = (Engine.compile code)
    for (inputStream, events) in inputs do
        match env.[inputStream] with
        | VStream stream -> CSVAdapter.FromString(stream, events) |> ignore
        | _ -> failwith "Input is not a stream" 
    { code = code; env = env; queues = LinkedList<testQueue>() }

let testsLeft test =
    Seq.to_list (Seq.concat [ for queue in test.queues -> queue.Values ])

(* TestCases *)

let parseTestFile fileName =
    let code, inputs = Array.fold_left (fun (c, i) (n:string) -> 
                                            if n.StartsWith("#!") 
                                                then (c, (n.Split([|' '|]).[1], "")::i)
                                                else if not (List.is_empty i)
                                                         then let hd = i.Head
                                                              (c, (fst hd, (snd hd) + n + "\n")::i.Tail)
                                                         else (c + n + "\r\n", i))
                                       ("", [])
                                       (File.ReadAllLines(fileName))
    code, inputs

type TestCaseAttribute(srcFile:string) =
    inherit Attribute()
    member self.CreateTest() =
        let code, inputs = parseTestFile (@"C:\streampy\prototypes\ezql\test\" + srcFile)
        init code inputs

let currentAssembly = Assembly.LoadFrom(Assembly.GetExecutingAssembly().GetName().Name + ".exe")

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
        try
            testMethod.Invoke(null, [|box test|]) |> ignore
        with
        | err -> printfn "%A" err
        Engine.mainLoop ()
        let left = testsLeft test
        if left.IsEmpty
            then printf "."
            else printfn "[%s] test(s) left: %A" testName left

        Engine.reset ()
        
*)