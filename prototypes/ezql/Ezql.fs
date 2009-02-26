#light

open System
open System.IO
open System.Text
open System.Collections.Generic
open System.Threading
open EzqlLexer
open EzqlParser
open EzqlAst
open Eval
open Types
open Adapters

module Engine =

    open Scheduler
    open Clock

    let clock = Scheduler.sched.clock

    let parse code =
        // let lexbuf = Lexing.from_text_reader Encoding.ASCII file
        let lexbuf = Lexing.from_string code
        try
            EzqlParser.start EzqlLexer.token lexbuf
        with e ->
            let pos = lexbuf.EndPos
            failwithf "Error near line %d, character %d\n" pos.Line pos.Column

    let compile code =
        let ast = parse code

        // Initial environment
        let mutable env = Map.of_list [("stream", VType "stream")]
        env <- match ast with
               | Prog stmts -> List.fold_left eval env stmts
        
        // Return just the streams
        Map.fold_left (fun (m:Map<string, IStream>) k v ->
                           match v with
                           | VStream stream -> m.Add(k, stream)
                           | _ -> m) 
                      (Map.empty<string, IStream>)
                      env
            
    let init =      
        Scheduler.init clock


module Main =

    open DateTimeExtensions

    let printStream (stream: IStream) name = 
        stream.OnAdd (fun t -> printfn "[%A] + %s: %A" Engine.clock.Now.TotalSeconds name t)
        stream.OnExpire (fun t -> printfn "[%A] - %s: %A" Engine.clock.Now.TotalSeconds name t)

    let printContValue (cv: IContValue) name =
        cv.OnSet (fun t -> printfn "[%A] + %s: %A" Engine.clock.Now.TotalSeconds name t) 
        cv.OnExpire (fun t -> printfn "[%A] - %s: %A" Engine.clock.Now.TotalSeconds name t) 
        

    Engine.init |> ignore
    let inputs = File.ReadAllText(Sys.argv.[1]) |> Engine.compile
    
  //  printStream inputs.["temp_readings"] "temp_readings"
 //   printContValue inputs.["currentTemp"] "currentTemp"
    
    let adapter = CSVAdapter.FromString(inputs.["temp_readings"],
                                            @"Timestamp, temperature
                                                      0,          30
                                                     10,          50")
    
    let virtualClock = Engine.clock :?> Clock.VirtualClock
    while virtualClock.HasNext () do
        virtualClock.Step ()

System.Console.ReadLine() |> ignore





