#light

open Test

[<TestCase ("dicts/where.ez")>]
let test_dictsWhere (test:Test) =


    test.AssertThat (In "hotRooms"
                      [SetKey "3" "45" (At 4)
                       SetKey "2" "50" (At 6)
                       DelKey "3"      (At 7)])


    test.AssertThat (In "hotRooms2"
                      [SetKey "3" "45" (At 4)
                       SetKey "2" "50" (At 6)
                       DelKey "3"      (At 7)])


    test.AssertThat (In "hotRooms3"
                      [SetKey "3" "45" (At 4)
                       SetKey "2" "50" (At 6)
                       DelKey "3"      (At 7)])

    test.AssertThat (In "hotRooms4"
                      [])

    test.AssertThat (In "hoterThanLast"
                      [SetKey "3" "45" (At 5)
                       DelKey "3"      (At 6)
                       SetKey "2" "50" (At 7)
                       SetKey "3" "30" (At 9)])

    test.AssertThat (In "hoterThanLast2"
                      [SetKey "3" "45" (At 5)
                       DelKey "3"      (At 6)
                       SetKey "2" "50" (At 7)
                       SetKey "3" "30" (At 9)])

    test.AssertThat (In "tempGTHalfHumidity"
                      [SetKey "3" "45" (At 4)
                       DelKey "3"      (At 5)
                       SetKey "3" "45" (At 6)
                       SetKey "2" "50" (At 6)
                       DelKey "3"      (At 7)])

    test.AssertThat (In "lastHoterThan25"
                      [SetKey "1" "25" (At 4)
                       SetKey "3" "45" (At 4)
                       DelKey "1"      (At 5)
                       DelKey "3"      (At 5)
                       SetKey "1" "25" (At 6)
                       SetKey "2" "50" (At 6)
                       SetKey "3" "45" (At 6)
                       SetKey "3" "30" (At 7)
                       DelKey "1"      (At 9)
                       DelKey "2"      (At 9)
                       DelKey "3"      (At 9)])

[<TestCase ("dicts/select.ez")>]
let test_dictsSelect (test:Test) =

    test.AssertThat (In "tempsPerRoomX2"
                      [SetKey "1" " 50" (At  2)
                       SetKey "3" " 90" (At  4)
                       SetKey "2" "100" (At  6)
                       SetKey "3" " 60" (At  7)
                       SetKey "1" " 46" (At  9)])

    test.AssertThat (In "tempsPerRoomX2b"
                      [SetKey "1" " 75" (At  2)
                       SetKey "1" " 95" (At  4)
                       SetKey "3" "135" (At  4)
                       SetKey "1" " 75" (At  5)
                       SetKey "3" "115" (At  5)
                       SetKey "1" "100" (At  6)
                       SetKey "2" "150" (At  6)
                       SetKey "3" "140" (At  6)
                       SetKey "1" " 80" (At  7)
                       SetKey "2" "130" (At  7)
                       SetKey "3" " 90" (At  7)
                       SetKey "1" " 69" (At  9)
                       SetKey "2" "123" (At  9)
                       SetKey "3" " 83" (At  9)])

    test.AssertThat (In "always3"
                      [SetKey "1" "3" (At  2)
                       SetKey "3" "3" (At  4)
                       SetKey "2" "3" (At  6)])

