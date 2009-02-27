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

open DateTimeExtensions

let printStream (stream: IStream) name = 
    stream.OnAdd (fun t -> printfn "[%A] + %s: %A" Engine.clock.Now.TotalSeconds name t)
    stream.OnExpire (fun t -> printfn "[%A] - %s: %A" Engine.clock.Now.TotalSeconds name t)

let printContValue (cv: IContValue) name =
    cv.OnSet (fun t -> printfn "[%A] + %s: %A" Engine.clock.Now.TotalSeconds name t) 
    cv.OnExpire (fun t -> printfn "[%A] - %s: %A" Engine.clock.Now.TotalSeconds name t) 
    
let streams = File.ReadAllText(Sys.argv.[1]) |> Engine.compile

//  printStream inputs.["temp_readings"] "temp_readings"
printStream streams.["currentTemp"] "currentTemp"

let adapter = CSVAdapter.FromString(streams.["temp_readings"],
                                    @"Timestamp, temperature
                                              0,          30
                                              1,          50")

let virtualClock = Engine.clock :?> Clock.VirtualClock
while virtualClock.HasNext () do
    virtualClock.Step ()

System.Console.ReadLine() |> ignore





