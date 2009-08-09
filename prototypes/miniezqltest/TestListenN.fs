open Test
open Types

[<TestCase ("listenN/listenN.ez")>]
let test_listenN (test:Test) =
  test.AssertThat (In "b"
                    [//Set " 0" (At 0) -> Initialization doesn't generate an event
                     Set "-1" (At  3)
                     Set " 0" (At  4)
                     Set " 1" (At  6)
                     Set " 0" (At  9)])

 
  (*
   * At 2, room 1 is created in state A
   * At 4, room 3 is created in state A, room 1 goes to state B
   * At 5, room 1 -> C, room 3 -> B
   * At 6, rooms 1 and 2 -> A, room 3 -> C
   * At 7, room 3 -> A
   *)                    
  test.AssertThat (In "roomsInStateA"
                    [SetKey "1" "1" (At 2)
                     SetKey "3" "3" (At 4)
                     DelKey "1"     (At 4)
                     DelKey "3"     (At 5)
                     SetKey "1" "1" (At 6)
                     SetKey "2" "2" (At 6)
                     SetKey "3" "3" (At 7)
                     DelKey "1"     (At 7)
                     DelKey "2"     (At 7)
                     DelKey "3"     (At 9)]) 


[<TestCase ("listenN/uda.ez")>]
let test_UDA (test:Test) =
  test.AssertThat (In "lastTemp_ema"
                    [Set "25.0"                                          (At 2)
                     Set "35.0"                                          (At 4)
                     Set "95 / 3.0"                                      (At 5)
                     Set "25 + 95 / 6.0"                                 (At 6)
                     Set "15 + (25 + 95 / 6.0) / 2.0"                    (At 7)
                     Set "23 / 2.0 + (15 + (25 + 95 / 6.0) / 2.0) / 2.0" (At 9)])
                     
                     
  test.AssertThat (In "lastTemp_3sec_count"
                    [Set "1" (At  2)
                     Set "2" (At  4)
                     Set "3" (At  5)
                     Set "4" (At  6)
                     Set "3" (At  8)
                     Set "2" (At  10)
                     Set "1" (At  12)])              


  test.AssertThat (In "lastTemp_3sec_sum"
                    [Set " 25" (At  2)
                     Set " 70" (At  4)
                     Set " 95" (At  5)
                     Set "145" (At  6)
                     Set "150" (At  7)
                     Set "105" (At  8)
                     Set "103" (At  9)
                     Set " 53" (At  10)
                     Set " 23" (At  12)])


  test.AssertThat (In "lastTemp_3sec_avg"
                    [Set "     25" (At  2)
                     Set "     35" (At  4)
                     Set " 95 / 3" (At  5)
                     Set "145 / 4" (At  6)
                     Set "150 / 4" (At  7)
                     Set "105 / 3" (At  8)
                     Set "103 / 3" (At  9)
                     Set " 53 / 2" (At  10)
                     Set "     23" (At  12)])                     