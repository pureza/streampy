#light

open System
open System.Reflection
open System.Collections.Generic
open System.IO
open Types
open Eval
open Adapters
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

let AssertThat(entity, (istream:IStream), (rstream:IStream), facts) =
    let queue = SortedList<DateTime, fact>(Map.of_list facts)
    let checkBuilder factType = 
        (fun ev ->
            let now = Engine.now ()
            while queue.Count > 0 && queue.Keys.[0] < now do
                printfn "[%s] Error: Predicted fact '%A' at %A didn't happen!" entity
                    queue.Values.[0] queue.Keys.[0].TotalSeconds
                queue.RemoveAt(0) |> ignore
            
            let fact = (factType ev)
            if queue.Count > 0 && queue.Keys.[0] = now
                then if queue.[now] = fact then queue.Remove(now) |> ignore
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
                        | VContinuousValue cv -> AssertThat(entity, cv.InsStream, cv.RemStream, facts)
                        | VMap assoc -> AssertThat(entity, assoc.InsStream, assoc.RemStream, facts)
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