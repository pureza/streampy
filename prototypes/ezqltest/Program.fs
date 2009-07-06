open System
open Ast
open Graph
open Dataflow
open Types
open Test

(*
Test.runTests [(Test.findTest "test_streamsWhere")]
Test.runTests [(Test.findTest "test_streamsSelect")]
Test.runTests [(Test.findTest "test_functions")]
Test.runTests [(Test.findTest "test_nonContFunctions")]
Test.runTests [(Test.findTest "test_entitiesMisc")]
Test.runTests [(Test.findTest "test_entitiesMisc3")]
Test.runTests [(Test.findTest "test_streamsGroupby")]; Test.runTests [(Test.findTest "test_dictsWhere")]; Test.runTests [(Test.findTest "test_dictsSelect")]
Test.runTests [(Test.findTest "test_aggregatesSum")]
Test.runTests [(Test.findTest "test_aggregatesMax")]
Test.runTests [(Test.findTest "test_aggregatesAny")]
Test.runTests [(Test.findTest "test_aggregatesAll")]
Test.runTests [(Test.findTest "test_listenN")]
Test.runTests [(Test.findTest "test_muggySimple")]
*)

//Test.runTests [(Test.findTest "test_entitiesMisc2")]


Test.runTests [(Test.findTest "test_lrb")]




Test.runTests (Test.findTests ())
    
Console.ReadLine() |> ignore

