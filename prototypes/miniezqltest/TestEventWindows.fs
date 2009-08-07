#light

open Test

[<TestCase ("eventWindows/create_window.ez")>]
let test_windowsCreation (test:Test) =

  test.AssertThat (In "temps_3secs"
                      [Added   "{ room_id = 1, temperature = 25, timestamp = 2 }" (At  2)
                       Added   "{ room_id = 3, temperature = 45, timestamp = 4 }" (At  4)
                       Added   "{ room_id = 1, temperature = 25, timestamp = 5 }" (At  5)
                       Expired "{ room_id = 1, temperature = 25, timestamp = 2 }" (At  5)
                       Added   "{ room_id = 2, temperature = 50, timestamp = 6 }" (At  6)
                       Added   "{ room_id = 3, temperature = 30, timestamp = 7 }" (At  7)
                       Expired "{ room_id = 3, temperature = 45, timestamp = 4 }" (At  7)
                       Expired "{ room_id = 1, temperature = 25, timestamp = 5 }" (At  8)
                       Added   "{ room_id = 1, temperature = 23, timestamp = 9 }" (At  9)
                       Expired "{ room_id = 2, temperature = 50, timestamp = 6 }" (At  9)
                       Expired "{ room_id = 3, temperature = 30, timestamp = 7 }" (At 10)
                       Expired "{ room_id = 1, temperature = 23, timestamp = 9 }" (At 12)]) 

  test.AssertThat (In "lastTemp"
                    [Set "25" (At  2)
                     Set "45" (At  4)
                     Set "25" (At  5)
                     Set "50" (At  6)
                     Set "30" (At  7)
                     Set "23" (At  9)])

  let rewriteTests = [Added   "{ room_id = 3, temperature = 45, timestamp = 4 }" (At  4)
                      Added   "{ room_id = 2, temperature = 50, timestamp = 6 }" (At  6)
                      Added   "{ room_id = 3, temperature = 30, timestamp = 7 }" (At  7)
                      Expired "{ room_id = 3, temperature = 45, timestamp = 4 }" (At  7)
                      Expired "{ room_id = 2, temperature = 50, timestamp = 6 }" (At  9)
                      Expired "{ room_id = 3, temperature = 30, timestamp = 7 }" (At 10)]

  test.AssertThat (In "hotTemps_3secs" rewriteTests)
  test.AssertThat (In "hotTemps_3secsc" rewriteTests)
                       
                       
  test.AssertThat (In "tempsPerRoom"
                    [SetKey "1" "25" (At  2)
                     SetKey "3" "45" (At  4)
                     SetKey "2" "50" (At  6)
                     SetKey "3" "30" (At  7)
                     SetKey "1" "23" (At  9)])

  test.AssertThat (In "temps_3secsX2"
                      [Set "90"   (At  4)
                       Set "60"   (At  7)
                       Set "null" (At 10)])                   



[<TestCase ("eventWindows/groupby.ez")>]
let test_windowGroupby (test:Test) =
  test.AssertThat (In "tempsPerRoom"
                    [SetKey "1"   "25" (At  2)
                     SetKey "3"   "45" (At  4)
                     SetKey "2"   "50" (At  6)
                     SetKey "3"   "30" (At  7)
                     SetKey "1" "null" (At  8)
                     SetKey "1"   "23" (At  9)
                     SetKey "2" "null" (At  9)
                     SetKey "3" "null" (At 10)
                     SetKey "1" "null" (At 12)])
                     
  test.AssertThat (In "allRooms20"
                      [SetKey "1" "20" (At  2)
                       SetKey "3" "20" (At  4)
                       SetKey "2" "20" (At  6)])

[<TestCase ("eventWindows/sortby.ez")>]
let test_sortBy (test:Test) = ()