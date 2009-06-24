#light

open Test

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