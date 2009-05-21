﻿#light

open System
open Ast
open Graph
open Dataflow
open Types
open Test

Test.runTests [(Test.findTest "test_entitiesMisc2")]

//Test.runTests [(Test.findTest "test_streamsGroupby")]
//Test.runTests [(Test.findTest "test_dictsWhere")]
//Test.runTests [(Test.findTest "test_dictsSelect")]

//Test.runTests (Test.findTests ())
//Test.runTests [(Test.findTest "test_aggregatesSum")]
//Test.runTests [(Test.findTest "test_entitiesMisc2")]
    
Console.ReadLine() |> ignore

