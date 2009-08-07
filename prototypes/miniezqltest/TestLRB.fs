open Test

[<TestCase ("lrb/lrb.ez")>]
let test_lrb (test:Test) =
  test.AssertThat (In "volume"
                     [SetKey "1" "1" (At 1)
                      SetKey "1" "2" (At 2)
                      SetKey "1" "1" (At 3)
                      SetKey "2" "1" (At 3)
                      SetKey "1" "2" (At 4)
                      SetKey "1" "1" (At 5)
                      SetKey "2" "2" (At 5)
                      SetKey "1" "0" (At 6)
                      SetKey "2" "3" (At 6)
                      SetKey "2" "2" (At 7)
                      SetKey "3" "1" (At 7)
                      SetKey "2" "1" (At 8)
                      SetKey "3" "2" (At 8)])


  test.AssertThat (In "total"
                     [SetKey "10" "1" (At 1)   // Vehicle 10 is in segment 1 which is congested with 1 vehicle
                      SetKey "20" "2" (At 2)   // Segment 1 continues to be congested
                      SetKey "30" "2" (At 4)   // Segment 1 is congested with 2 vehicles
                      SetKey "10" "2" (At 7)   // Segment 3 is congested with vehicle 10
                      SetKey "20" "4" (At 8)]) // Segment 3 is co ngested with vehicles 10 and 20


  test.AssertThat (In "total2"
                     [SetKey "10" "1" (At 1)
                      SetKey "20" "2" (At 2)
                      SetKey "30" "2" (At 4)
                      SetKey "10" "2" (At 7)
                      SetKey "20" "4" (At 8)]) 


[<TestCase ("lrb/lrbFixed.ez")>]
let test_lrbFixed (test:Test) =
  test.AssertThat (In "total"
                     [SetKey "10" "0" (At 1)
                      SetKey "20" "2" (At 2)   // Segment 1 is congested when vehicle 2 arrives
                      SetKey "30" "2" (At 4)
                      SetKey "20" "4" (At 8)]) // Segment 3 is congested when vehicle 3 arrives
