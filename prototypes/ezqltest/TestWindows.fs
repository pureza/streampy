#light

open Test

[<TestCase ("windows/create_window.ez")>]
let test_windowsCreation (test:Test) =

  test.AssertThat (In "temps_3secs"
                      [Added   2 "{ :room_id = 1, :temperature = 25 }" (At  2)
                       Added   4 "{ :room_id = 3, :temperature = 45 }" (At  4)
                       Added   5 "{ :room_id = 1, :temperature = 25 }" (At  5)
                       Expired 2 "{ :room_id = 1, :temperature = 25 }" (At  5)
                       Added   6 "{ :room_id = 2, :temperature = 50 }" (At  6)
                       Added   7 "{ :room_id = 3, :temperature = 30 }" (At  7)
                       Expired 4 "{ :room_id = 3, :temperature = 45 }" (At  7)
                       Expired 5 "{ :room_id = 1, :temperature = 25 }" (At  8)
                       Added   9 "{ :room_id = 1, :temperature = 23 }" (At  9)
                       Expired 6 "{ :room_id = 2, :temperature = 50 }" (At  9)
                       Expired 7 "{ :room_id = 3, :temperature = 30 }" (At 10)
                       Expired 9 "{ :room_id = 1, :temperature = 23 }" (At 12)]) 

  test.AssertThat (In "lastTemp"
                    [Set "25" (At  2)
                     Set "45" (At  4)
                     Set "25" (At  5)
                     Set "50" (At  6)
                     Set "30" (At  7)
                     Set "23" (At  9)])

  let rewriteTests = [Added   4 "{ :room_id = 3, :temperature = 45 }" (At  4)
                      Added   6 "{ :room_id = 2, :temperature = 50 }" (At  6)
                      Added   7 "{ :room_id = 3, :temperature = 30 }" (At  7)
                      Expired 4 "{ :room_id = 3, :temperature = 45 }" (At  7)
                      Expired 6 "{ :room_id = 2, :temperature = 50 }" (At  9)
                      Expired 7 "{ :room_id = 3, :temperature = 30 }" (At 10)]

  test.AssertThat (In "hotTemps_3secs" rewriteTests)
  test.AssertThat (In "hotTemps_3secsb" rewriteTests)
  test.AssertThat (In "hotTemps_3secsc" rewriteTests)
                       
                       
  test.AssertThat (In "tempsPerRoom"
                    [SetKey "1" "25" (At  2)
                     SetKey "3" "45" (At  4)
                     SetKey "2" "50" (At  6)
                     SetKey "3" "30" (At  7)
                     SetKey "1" "23" (At  9)])


[<TestCase ("windows/groupby.ez")>]
let test_windowGroupby (test:Test) =
  test.AssertThat (In "tempsPerRoom"
                    [SetKey "1" "25" (At  2)
                     SetKey "3" "45" (At  4)
                     SetKey "2" "50" (At  6)
                     SetKey "3" "30" (At  7)
                     SetKey "1" "23" (At  9)])
                     
  test.AssertThat (In "allRooms20"
                      [SetKey "1" "20" (At  2)
                       SetKey "3" "20" (At  4)
                       SetKey "2" "20" (At  6)])
