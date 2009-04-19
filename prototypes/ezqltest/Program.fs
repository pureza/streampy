#light

open System
open Ast
open Graph
open Dataflow
open Types
open Test
(*

let streams, allOps = Engine.compile @"
  temp_readings = stream (:room_id, :temperature);
  //hum_readings = stream (:room_id, :humidity);

  x = temp_readings.last(:room_id);
  //y = temp_readings.last(:temperature);

  //z = x + y * 3;
  //hot_readings = temp_readings.where(ev -> ev.temperature > x + y - x);

  //lastHot = hot_readings.last(:temperature);



  //a = (y - x + (x * temp_readings.last(:temperature)));

  //y = temp_readings.last(:temperature);

  //z = y * (x + x[3 min].max());

  //h = hum_readings.last(:humidity);
  
  tempPerRoom = temp_readings.groupby(:room_id, g -> g.last(:temperature));
  
  hotRooms = tempPerRoom.where(t -> t > 20);
  
  //hotRooms = tempPerRoom.where(t -> t > y);
  
  
  tempPerRoomX2 = tempPerRoom.select(t -> { :ola = temp_readings.last(:room_id) + t > 2 * t, :ole = t, 
                                            :oli = x });

  blah = { :a = x * 2 + x, :b = x + 5 };
   "
streams.["temp_readings"] (Event (DateTime.Now, Map.of_list [("room_id", VInt 1); ("temperature", VInt 30)]))
streams.["temp_readings"] (Event (DateTime.Now, Map.of_list [("room_id", VInt 1); ("temperature", VInt 40)]))
streams.["temp_readings"] (Event (DateTime.Now, Map.of_list [("room_id", VInt 2); ("temperature", VInt 20)]))
//streams.["humidity"] (Event (DateTime.Now, Map.of_list [("room_id", VInt 1); ("humidity", VInt 60)]))

//printfn "%A" allOps.["lastHot"].Value
printfn "%A" allOps.["tempPerRoom"].Value
printfn "%A" allOps.["hotRooms"].Value
printfn "%A" allOps.["tempPerRoomX2"].Value
printfn "%O" allOps.["blah"].Value


*)



Test.runTests (Test.findTests ())

//Test.runTests [(Test.findTest "test_dictsSelect")]
    
Console.ReadLine() |> ignore


(*
[<TestCase ("test1.ez")>]
let simpleTest (test:Test) =             
    test.AssertThat (In "hot"
                      [ExpiredAll (After 3)
                       Added 0 "{ :temp = 30, :blah = 33 }" (At  0)
                       Added 4 "{ :temp = 50, :blah = 33 }" (At  4)]) 
 
 
[<TestCase ("testCV.ez")>]
let testCV (test:Test) =   
        
    test.AssertThat (In "currentTemp"
                      [Set "30" (At 0)
                       Set "15" (At 3)
                       Set "50" (At 4)]) 

    test.AssertThat (In "currTempPlus5"
                      [Set "35" (At 0)
                       Set "20" (At 3)
                       Set "55" (At 4)]) 

    test.AssertThat (In "sumTemp"
                      [Set "30" (At 0)
                       Set "45" (At 3)
                       Set "95" (At 4)
                       Set "65" (At 5)
                       Set "50" (At 8)
                       Set " 0" (At 9)])   

    test.AssertThat (In "pastTemps"
                      [Set "30" (At 0)
                       Set "15" (At 3)
                       Set "50" (At 4)
                       Del 0 "30" (At 8)
                       Del 3 "15" (At 9)]) 

    test.AssertThat (In "pastTempsPlus5"
                      [Set "35" (At 0)
                       Set "20" (At 3)
                       Set "55" (At 4)
                       Del 0 "35" (At 8)
                       Del 3 "20" (At 9)]) 

    test.AssertThat (In "sumPastTemps"
                      [Set "30" (At 0)
                       Set "45" (At 3)
                       Set "95" (At 4)
                       Set "65" (At 8)
                       Set "50" (At 9)]) 


[<TestCase ("testGroupBy.ez")>]
let testGroupBy (test:Test) =     
    (*    
    test.AssertThat (In "temp_readings"
                      [Added 0 "{ :room_id = 1, :temperature = 30 }" (At  0)
                       Added 3 "{ :room_id = 2, :temperature = 15 }" (At  3)
                       Added 4 "{ :room_id = 1, :temperature = 50 }" (At  4)
                       Added 5 "{ :room_id = 1, :temperature = -40 }" (At  5)])
 
    test.AssertThat (In "sumPerRoom[1]"
                      [Set "30" (At  0)
                       Set "80" (At  4)
                       Set "40" (At 5)])
*)



    test.AssertThat (In "sumPerRoomBiggerThan50"
                      [AddKey "1" (At  4)
                       DelKey "1" (At  5)
                       AddKey "1" (At  6)
                       DelKey "1" (At  9)])

    test.AssertThat (In "sumPerRoomBiggerThan50_3secs"
                      [AddKey "1" (At  4)
                       DelKey "1" (At  12)])

    test.AssertThat (In "maxPerRoomBiggerThan50"
                      [AddKey "1" (At  4)])

    test.AssertThat (In "maxPerRoomBiggerThan50_3secs"
                      [AddKey "1" (At  4)])
                                         
    test.AssertThat (In "hotRooms"
                      [AddKey "1" (At  4)
                       DelKey "1" (At  5)
                       AddKey "1" (At  7)
                       DelKey "1" (At  8)])  
                       
    test.AssertThat (In "hotAndWasHotter"
                      [AddKey "1" (At  7)
                       DelKey "1" (At  8)])                 


[<TestCase ("testMERDA.ez")>]
let testDiamond (test:Test) =             
    test.AssertThat (In "d"
                       [Set   "60" (At 0)
                        Set  "120" (At 4)
                        Set  "-80" (At 5)
                        Set   "80" (At 6)
                        Set  "140" (At 7)
                        Set "-140" (At 8)]) 
                        
//Test.runTests (Test.findTests ())

//Test.runTests [(Test.findTest "simpleTest")]
//Test.runTests [(Test.findTest "testCV")]
//Test.runTests [(Test.findTest "testGroupBy")]

Test.runTests [(Test.findTest "testDiamond")]

//Test.runTests [(Test.findTest "testCV2")]




Console.ReadLine() |> ignore
//let graph = edges.ToAdjacencyGraph(edges)

*)