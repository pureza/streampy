#light

open System
open Ast
open Graph
open Dataflow
open Types
open Test

//Test.runTests (Test.findTests ())
//Test.runTests [(Test.findTest "test_aggregatesSum")]
Test.runTests [(Test.findTest "test_entitiesMisc2")]
    
Console.ReadLine() |> ignore

