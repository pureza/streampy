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
