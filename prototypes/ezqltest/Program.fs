#light

open System
open System.Reflection
open Test

[<TestCase ("test1.ez")>]
let simpleTest (test:Test) =             
    test.AssertThat (In "hot"
                      [ExpiredAll (After 3)
                       Added 0 "{ :temp = 30, :blah = 33 }" (At  0)
                       Added 4 "{ :temp = 50, :blah = 33 }" (At  4)])


    test.AssertThat (In "currentTemp"
                      [Set "30" (At 0)
                       Set "15" (At 3)
                       Set "50" (At 4)]) 

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

    test.AssertThat (In "sumPastTemps"
                      [Set "30" (At 0)
                       Set "45" (At 3)
                       Set "95" (At 4)
                       Set "65" (At 8)
                       Set "50" (At 9)]) 
 
 
[<TestCase ("testGroupBy.ez")>]
let testGroupBy (test:Test) =     
        
    test.AssertThat (In "temp_readings"
                      [Added 0 "{ :room_id = 1, :temperature = 30 }" (At  0)
                       Added 3 "{ :room_id = 2, :temperature = 15 }" (At  3)
                       Added 4 "{ :room_id = 1, :temperature = 50 }" (At  4)
                       Added 5 "{ :room_id = 1, :temperature = -40 }" (At  5)])
 
    test.AssertThat (In "sumPerRoom[1]"
                      [Set "30" (At  0)
                       Set "80" (At  4)
                       Set "40" (At 5)])

    test.AssertThat (In "sumPerRoomBiggerThan50"
                      [Set "1" (At  4)
                       Del 5 "1" (At  5)])
                       
 
Test.runTests (Test.findTests ())

//Test.runTests [(Test.findTest "testGroupBy")]

Console.ReadLine() |> ignore