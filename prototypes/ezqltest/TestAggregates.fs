#light

open Test
open Types

[<TestCase ("aggregates/sum.ez")>]
let test_aggregatesSum (test:Test) =

    test.AssertThat (In "tempsSum"
                      [Set " 25" (At 2)
                       Set " 70" (At 4)
                       Set " 95" (At 5)
                       Set "145" (At 6)
                       Set "175" (At 7)
                       Set "198" (At 9)])
       
    test.AssertThat (In "tempsSum_3secs"
                      [Set " 25" (At  2)
                       Set " 70" (At  4)
                       Set "120" (At  6)
                       Set "105" (At  7)
                       Set " 80" (At  8)
                       Set " 53" (At  9)
                       Set " 23" (At 10)
                       Set "  0" (At 12)])   
                       
    test.AssertThat (In "sumTempsPerRoom"
                      [SetKey "1" "25" (At  2)
                       SetKey "3" "45" (At  4)
                       SetKey "1" "50" (At  5)
                       SetKey "2" "50" (At  6)
                       SetKey "3" "75" (At  7)
                       SetKey "1" "73" (At  9)])                                    

    test.AssertThat (In "sumTempsPerRoomX2"
                      [SetKey "1" " 50" (At  2)
                       SetKey "3" " 90" (At  4)
                       SetKey "1" "100" (At  5)
                       SetKey "2" "100" (At  6)
                       SetKey "3" "150" (At  7)
                       SetKey "1" "146" (At  9)])  

    test.AssertThat (In "sumTempsPerRoom_3secs"
                      [SetKey "1" "25" (At  2)
                       SetKey "3" "45" (At  4)
                       SetKey "2" "50" (At  6)
                       SetKey "3" "30" (At  7)
                       SetKey "1" " 0" (At  8)
                       SetKey "1" "23" (At  9)
                       SetKey "2" " 0" (At  9)
                       SetKey "3" " 0" (At 10)
                       SetKey "1" " 0" (At 12)])

    test.AssertThat (In "sumTempsPerRoom_3secsb"
                      [SetKey "1" "25" (At  2)
                       SetKey "3" "45" (At  4)
                       SetKey "2" "50" (At  6)
                       SetKey "3" "30" (At  7)
                       SetKey "1" " 0" (At  8)
                       SetKey "1" "23" (At  9)
                       SetKey "2" " 0" (At  9)
                       SetKey "3" " 0" (At 10)
                       SetKey "1" " 0" (At 12)])
                       
    test.AssertThat (In "test_init"
                      [SetKey "1" "25" (At  2)
                       SetKey "3" "45" (At  4)
                       SetKey "2" "50" (At  6)
                       SetKey "3" "30" (At  7)
                       SetKey "1" " 0" (At  8)
                       SetKey "1" "23" (At  9)
                       SetKey "2" " 0" (At  9)
                       SetKey "3" " 0" (At 10)
                       SetKey "1" " 0" (At 12)])


[<TestCase ("aggregates/max.ez")>]
let test_aggregatesMax (test:Test) =
  test.AssertThat (In "a"
                    [SetKey "1" "25" (At  2)
                     SetKey "1" "45" (At  4)
                     SetKey "3" "45" (At  4)
                     SetKey "1" "50" (At  6)
                     SetKey "2" "50" (At  6)
                     SetKey "3" "50" (At  6)
                     SetKey "1" "30" (At  8)
                     SetKey "2" "30" (At  8)
                     SetKey "3" "30" (At  8)
                     SetKey "1" "23" (At  9)
                     SetKey "2" "23" (At  9)
                     SetKey "3" "23" (At  9)
                     SetKeyRaw "1" VNull (At 11)
                     SetKeyRaw "2" VNull (At 11)
                     SetKeyRaw "3" VNull (At 11)])

  test.AssertThat (In "b"
                    [SetKey "1" "25" (At  2)
                     SetKey "1" "45" (At  4)
                     SetKey "3" "45" (At  4)
                     SetKey "1" "50" (At  6)
                     SetKey "2" "50" (At  6)
                     SetKey "3" "50" (At  6)])

  test.AssertThat (In "c"
                    [SetKey "1" "25" (At  2)
                     SetKey "3" "45" (At  4)
                     SetKey "2" "50" (At  6)])                    

  test.AssertThat (In "d"
                    [SetKey    "1"  "25" (At  2)
                     SetKeyRaw "1" VNull (At  4)
                     SetKey    "3"  "45" (At  4)
                     SetKey    "1"  "25" (At  5)
                     SetKey    "2"  "50" (At  6)
                     SetKeyRaw "3" VNull (At  6)
                     SetKeyRaw "1" VNull (At  7)
                     SetKey    "3"  "30" (At  7)
                     SetKeyRaw "2" VNull (At  8)
                     SetKey    "1"  "23" (At  9)
                     SetKeyRaw "3" VNull (At  9)
                     SetKeyRaw "1" VNull (At 11)])                     

  test.AssertThat (In "e"
                    [SetKey "1" "25" (At  2)
                     SetKey "3" "45" (At  4)
                     SetKey "2" "50" (At  6)])

  test.AssertThat (In "f"
                    [SetKey    "1"  "25" (At  2)
                     SetKeyRaw "1" VNull (At  4)
                     SetKey    "3"  "45" (At  4)
                     SetKey    "1"  "25" (At  5)
                     SetKey    "2"  "50" (At  6)
                     SetKeyRaw "3" VNull (At  6)
                     SetKeyRaw "1" VNull (At  7)
                     SetKey    "3"  "30" (At  7)
                     SetKeyRaw "2" VNull (At  8)
                     SetKey    "1"  "23" (At  9)
                     SetKeyRaw "3" VNull (At  9)
                     SetKeyRaw "1" VNull (At 11)])                       

  test.AssertThat (In "g"
                    [SetKey "1" "25" (At  2)
                     SetKey "3" "45" (At  4)
                     SetKey "2" "50" (At  6)])                     

  test.AssertThat (In "h"
                    [SetKey "1" "25" (At  2)
                     SetKey "3" "45" (At  4)
                     SetKey "2" "50" (At  6)
                     SetKey "3" "30" (At  9)
                     SetKey "1" "23" (At 11)])


[<TestCase ("aggregates/any.ez")>]
let test_aggregatesAny (test:Test) =
  test.AssertThat (In "a"
                    [Set "false" (At  2)
                     Set "true"  (At  5)
                     Set "false" (At  6)
                     Set "true"  (At  7)
                     Set "false" (At 11)])

  test.AssertThat (In "b"
                     [Set "false" (At 2)
                      Set "true"  (At 6)])
                     
  test.AssertThat (In "c"
                    [Set "false" (At 2)
                     Set "true"  (At 5)
                     Set "false" (At 6)
                     Set "true"  (At 7)
                     Set "false" (At 8)])

  test.AssertThat (In "d"
                     [Set "false" (At 2)
                      Set "true"  (At 4)])
                     
  test.AssertThat (In "e"
                    [Set "false" (At 2)
                     Set "true"  (At 4)
                     Set "false" (At 9)])

  test.AssertThat (In "f"
                    [SetKey "1" "false" (At  2)
                     SetKey "3" "true"  (At  4)
                     SetKey "2" "true"  (At  6)])


[<TestCase ("aggregates/all.ez")>]
let test_aggregatesAll (test:Test) =
  test.AssertThat (In "a"
                    [Set "true"  (At  2)
                     Set "false" (At  4)
                     Set "true"  (At  5)
                     Set "false" (At  6)
                     Set "true"  (At  8)])                     

  test.AssertThat (In "b"
                    [Set "true" (At 2)
                     Set "false" (At 9)])                     