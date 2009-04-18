#light

open Test

[<TestCase ("dicts/where.ez")>]
let test_dictsWhere (test:Test) =

(*
    test.AssertThat (In "hotRooms"
                      [SetKey "3" "45" (At 4)
                       SetKey "2" "50" (At 6)
                       DelKey "3"      (At 7)])
*)

    test.AssertThat (In "hotRooms2"
                      [SetKey "3" "45" (At 4)
                       SetKey "2" "50" (At 6)
                       DelKey "3"      (At 7)])

