#light

open Test

[<TestCase ("windows/create_window.ez")>]
let test_windowsCreation (test:Test) =

    test.AssertThat (In "temps_2secs"
                      [Added   2 "{ :room_id = 1, :temperature = 25 }" (At  2)
                       Expired 2 "{ :room_id = 1, :temperature = 25 }" (At  4)
                       Added   4 "{ :room_id = 3, :temperature = 45 }" (At  4)
                       Expired 4 "{ :room_id = 3, :temperature = 45 }" (At  6)])

(*
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
*)