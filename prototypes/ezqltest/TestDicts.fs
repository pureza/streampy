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

    test.AssertThat (In "lastTempAllRooms"
                      [SetKey "1" "25" (At  2)
                       SetKey "1" "45" (At  4)
                       SetKey "3" "45" (At  4)
                       SetKey "1" "25" (At  5)
                       SetKey "3" "25" (At  5)
                       SetKey "1" "50" (At  6)
                       SetKey "2" "50" (At  6)
                       SetKey "3" "50" (At  6)
                       SetKey "1" "30" (At  7)
                       SetKey "2" "30" (At  7)
                       SetKey "3" "30" (At  7)
                       SetKey "1" "23" (At  9)
                       SetKey "2" "23" (At  9)
                       SetKey "3" "23" (At  9)])

    test.AssertThat (In "someRecordPerRoom"
                      [SetKey "1" "{ :a = 5, :b = 25, :c = 90, :d = 140 }" (At  2)
                       SetKey "1" "{ :a = 5, :b = 25, :c = 71, :d = 121 }" (At  3)
                       SetKey "1" "{ :a = 5, :b = 25, :c = 81, :d = 151 }" (At  4)
                       SetKey "3" "{ :a = 5, :b = 45, :c = 81, :d = 171 }" (At  4)
                       SetKey "1" "{ :a = 5, :b = 25, :c = 91, :d = 141 }" (At  5)
                       SetKey "3" "{ :a = 5, :b = 45, :c = 91, :d = 161 }" (At  5)
                       SetKey "1" "{ :a = 5, :b = 25, :c = 72, :d = 147 }" (At  6)
                       SetKey "2" "{ :a = 5, :b = 50, :c = 72, :d = 172 }" (At  6)
                       SetKey "3" "{ :a = 5, :b = 45, :c = 72, :d = 167 }" (At  6)
                       SetKey "1" "{ :a = 5, :b = 25, :c = 82, :d = 137 }" (At  7)
                       SetKey "2" "{ :a = 5, :b = 50, :c = 82, :d = 162 }" (At  7)
                       SetKey "3" "{ :a = 5, :b = 30, :c = 82, :d = 142 }" (At  7)
                       SetKey "1" "{ :a = 5, :b = 25, :c = 92, :d = 147 }" (At  8)
                       SetKey "2" "{ :a = 5, :b = 50, :c = 92, :d = 172 }" (At  8)
                       SetKey "3" "{ :a = 5, :b = 30, :c = 92, :d = 152 }" (At  8)
                       SetKey "1" "{ :a = 5, :b = 23, :c = 92, :d = 138 }" (At  9)
                       SetKey "2" "{ :a = 5, :b = 50, :c = 92, :d = 165 }" (At  9)
                       SetKey "3" "{ :a = 5, :b = 30, :c = 92, :d = 145 }" (At  9)])



    test.AssertThat (In "hotRoomsX2"
                      [SetKey "1" " 50" (At 4)
                       SetKey "3" " 90" (At 4)
                       DelKey "1"       (At 5)
                       DelKey "3"       (At 5)
                       SetKey "1" " 50" (At 6)
                       SetKey "2" "100" (At 6)
                       SetKey "3" " 90" (At 6)
                       SetKey "3" " 60" (At 7)
                       DelKey "1"       (At 9)
                       DelKey "2"       (At 9)
                       DelKey "3"       (At 9)])


    test.AssertThat (In "weird"
                      [SetKey "1" "45" (At 4)
                       SetKey "3" "45" (At 4)
                       DelKey "1"      (At 5)
                       DelKey "3"      (At 5)
                       SetKey "1" "50" (At 6)
                       SetKey "2" "50" (At 6)
                       SetKey "3" "50" (At 6)
                       SetKey "1" "30" (At 7)
                       SetKey "2" "30" (At 7)
                       SetKey "3" "30" (At 7)
                       DelKey "1"      (At 9)
                       DelKey "2"      (At 9)
                       DelKey "3"      (At 9)])

    test.AssertThat (In "nestedSelect"
                      [SetKey "1" "{ :a = 3, :b = 45 }" (At 4)
                       SetKey "3" "{ :a = 3, :b = 45 }" (At 4)
                       DelKey "1"                       (At 5)
                       DelKey "3"                       (At 5)
                       SetKey "1" "{ :a = 3, :b = 50 }" (At 6)
                       SetKey "2" "{ :a = 3, :b = 50 }" (At 6)
                       SetKey "3" "{ :a = 3, :b = 50 }" (At 6)
                       SetKey "1" "{ :a = 3, :b = 30 }" (At 7)
                       SetKey "2" "{ :a = 3, :b = 30 }" (At 7)
                       SetKey "3" "{ :a = 3, :b = 30 }" (At 7)
                       DelKey "1"                       (At 9)
                       DelKey "2"                       (At 9)
                       DelKey "3"                       (At 9)])