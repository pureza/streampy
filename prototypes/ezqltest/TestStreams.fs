#light

open Test

[<TestCase ("streams/where.ez")>]
let test_streamsWhere (test:Test) =
    let everything = [Added 0 "{ :temperature = 30 }" (At  0)
                      Added 3 "{ :temperature = 15 }" (At  3)
                      Added 4 "{ :temperature = 50 }" (At  4)]

    test.AssertThat (In "hot_readings"
                      [Added 0 "{ :temperature = 30 }" (At  0)
                       Added 4 "{ :temperature = 50 }" (At  4)]) 

    test.AssertThat (In "all_readings" everything) 
    
    test.AssertThat (In "all_readings2" everything) 

    test.AssertThat (In "wet_readings"
                      [Added 3 "{ :temperature = 15 }" (At  3)
                       Added 4 "{ :temperature = 50 }" (At  4)]) 
                       
    test.AssertThat (In "hot_wet_readings"
                      [Added 3 "{ :temperature = 15 }" (At  3)
                       Added 4 "{ :temperature = 50 }" (At  4)]) 

    test.AssertThat (In "lastTemp"
                      [Set "30" (At  0)
                       Set "15" (At  3)
                       Set "50" (At  4)]) 


[<TestCase ("streams/groupby.ez")>]
let test_streamsGroupby (test:Test) =

    test.AssertThat (In "tempsPerRoom"
                      [SetKey "1" "25" (At  2)
                       SetKey "3" "45" (At  4)
                       SetKey "2" "50" (At  6)
                       SetKey "3" "30" (At  7)
                       SetKey "1" "23" (At  9)]) 

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

    test.AssertThat (In "allRooms20"
                      [SetKey "1" "20" (At  2)
                       SetKey "3" "20" (At  4)
                       SetKey "2" "20" (At  6)])

    test.AssertThat (In "tempsPerRoom2"
                      [SetKey "1" "25" (At  2)
                       SetKey "3" "45" (At  4)
                       SetKey "2" "50" (At  6)
                       SetKey "3" "30" (At  7)
                       SetKey "1" "23" (At  9)]) 
                       
    test.AssertThat (In "lastHumPerRoom"
                      [SetKey "1" "90" (At  2)
                       SetKey "1" "71" (At  3)
                       SetKey "1" "81" (At  4)
                       SetKey "3" "81" (At  4)
                       SetKey "1" "91" (At  5)
                       SetKey "3" "91" (At  5)
                       SetKey "1" "72" (At  6)
                       SetKey "2" "72" (At  6)
                       SetKey "3" "72" (At  6)
                       SetKey "1" "82" (At  7)
                       SetKey "2" "82" (At  7)
                       SetKey "3" "82" (At  7)
                       SetKey "1" "92" (At  8)
                       SetKey "2" "92" (At  8)
                       SetKey "3" "92" (At  8)])


    test.AssertThat (In "lastHumLastTempPerRoom"
                      [SetKey "1" "115" (At  2)
                       SetKey "1" " 96" (At  3)
                       SetKey "1" "106" (At  4)
                       SetKey "3" "126" (At  4)
                       SetKey "1" "116" (At  5)
                       SetKey "3" "136" (At  5)
                       SetKey "1" " 97" (At  6)
                       SetKey "2" "122" (At  6)
                       SetKey "3" "117" (At  6)
                       SetKey "1" "107" (At  7)
                       SetKey "2" "132" (At  7)
                       SetKey "3" "112" (At  7)
                       SetKey "1" "117" (At  8)
                       SetKey "2" "142" (At  8)
                       SetKey "3" "122" (At  8)
                       SetKey "1" "115" (At  9)]) 


    test.AssertThat (In "someRecordPerRoom"
                      [SetKey "1" "{ :a = 5, :b = 25, :c = 90, :d = 115 }" (At  2)
                       SetKey "1" "{ :a = 5, :b = 25, :c = 71, :d =  96 }" (At  3)
                       SetKey "1" "{ :a = 5, :b = 25, :c = 81, :d = 106 }" (At  4)
                       SetKey "3" "{ :a = 5, :b = 45, :c = 81, :d = 126 }" (At  4)
                       SetKey "1" "{ :a = 5, :b = 25, :c = 91, :d = 116 }" (At  5)
                       SetKey "3" "{ :a = 5, :b = 45, :c = 91, :d = 136 }" (At  5)
                       SetKey "1" "{ :a = 5, :b = 25, :c = 72, :d =  97 }" (At  6)
                       SetKey "3" "{ :a = 5, :b = 45, :c = 72, :d = 117 }" (At  6)
                       SetKey "2" "{ :a = 5, :b = 50, :c = 72, :d = 122 }" (At  6)
                       SetKey "1" "{ :a = 5, :b = 25, :c = 82, :d = 107 }" (At  7)
                       SetKey "2" "{ :a = 5, :b = 50, :c = 82, :d = 132 }" (At  7)
                       SetKey "3" "{ :a = 5, :b = 30, :c = 82, :d = 112 }" (At  7)
                       SetKey "1" "{ :a = 5, :b = 25, :c = 92, :d = 117 }" (At  8)
                       SetKey "2" "{ :a = 5, :b = 50, :c = 92, :d = 142 }" (At  8)
                       SetKey "3" "{ :a = 5, :b = 30, :c = 92, :d = 122 }" (At  8)
                       SetKey "1" "{ :a = 5, :b = 23, :c = 92, :d = 115 }" (At  9)])
