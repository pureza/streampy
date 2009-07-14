#light

open Test


[<TestCase ("streams/where.ez")>]
let test_streamsWhere (test:Test) =
  let everything = [Added 2 "{ room_id = 1, temperature = 25 }" (At  2)
                    Added 4 "{ room_id = 3, temperature = 45 }" (At  4)
                    Added 5 "{ room_id = 1, temperature = 25 }" (At  5)
                    Added 6 "{ room_id = 2, temperature = 50 }" (At  6)
                    Added 7 "{ room_id = 3, temperature = 30 }" (At  7)
                    Added 9 "{ room_id = 1, temperature = 23 }" (At  9)]

  test.AssertThat (In "hot_readings"
                    [Added 4 "{ room_id = 3, temperature = 45 }" (At  4)
                     Added 6 "{ room_id = 2, temperature = 50 }" (At  6)]) 

  test.AssertThat (In "all_readings" everything) 
  
  test.AssertThat (In "all_readings2" everything) 

  test.AssertThat (In "wet_readings"
                    [Added 2 "{ room_id = 1, temperature = 25 }" (At  2)
                     Added 5 "{ room_id = 1, temperature = 25 }" (At  5)
                     Added 9 "{ room_id = 1, temperature = 23 }" (At  9)]) 
                     
  test.AssertThat (In "hot_wet_readings"
                    [Added 2 "{ room_id = 1, temperature = 25 }" (At  2)
                     Added 5 "{ room_id = 1, temperature = 25 }" (At  5)
                     Added 9 "{ room_id = 1, temperature = 23 }" (At  9)]) 

  test.AssertThat (In "lastTemp"
                    [Set "25" (At  2)
                     Set "45" (At  4)
                     Set "25" (At  5)
                     Set "50" (At  6)
                     Set "30" (At  7)
                     Set "23" (At  9)])
                     
  test.AssertThat (In "hot_or_wet"
                     [Added 2 "{ room_id = 3, temperature = null, humidity = 90 }" (At  2)
                      Added 4 "{ room_id = 3, temperature = 45, humidity = null }" (At  4)
                      Added 5 "{ room_id = 3, temperature = null, humidity = 91 }" (At  5)
                      Added 6 "{ room_id = 2, temperature = 50, humidity = null }" (At  6)
                      Added 8 "{ room_id = 3, temperature = null, humidity = 92 }" (At  8)]) 
                     
  test.AssertThat (In "minTempRoom1"
                    [Set "25" (At  2)])

  test.AssertThat (In "minTempRoom1b"
                    [Set "25" (At  2)])                                         

  test.AssertThat (In "eventsPerRoomc"
                    [SetKey "1" "{ a = 25 }" (At  2)
                     SetKey "3" "{ a = 45 }" (At  4)
                     SetKey "2" "{ a = 50 }" (At  6)
                     SetKey "3" "{ a = 30 }" (At  7)])      
                    
  test.AssertThat (In "temp_plus_hum"
                    [Set " 70" (At 0)
                     Set "150" (At 1)
                     Set "175" (At 2)
                     Set "246" (At 3)
                     Set "291" (At 4)
                     Set "316" (At 5)
                     Set "366" (At 6)
                     Set "396" (At 7)
                     Set "488" (At 8)
                     Set "511" (At 9)])
                     
  test.AssertThat (In "eventsPerRoomd"
                    [SetKey "1" "25" (At  2)
                     SetKey "3" "45" (At  4)
                     SetKey "2" "50" (At  6)
                     SetKey "3" "30" (At  7)])                  

  test.AssertThat (In "where_with_listenN"
                    [Added 5 "{ room_id = 1, temperature = 25 }" (At  5)
                     Added 6 "{ room_id = 2, temperature = 50 }" (At  6)
                     Added 7 "{ room_id = 3, temperature = 30 }" (At  7)])


  test.AssertThat (In "hotTemps_3secsb"
                     [Added   4 "{ room_id = 3, temperature = 45 }" (At  4)
                      Added   6 "{ room_id = 2, temperature = 50 }" (At  6)
                      Added   7 "{ room_id = 3, temperature = 30 }" (At  7)
                      Expired 4 "{ room_id = 3, temperature = 45 }" (At  7)
                      Expired 6 "{ room_id = 2, temperature = 50 }" (At  9)
                      Expired 7 "{ room_id = 3, temperature = 30 }" (At 10)])

  test.AssertThat (In "temps_3secsX2b"
                      [Set "90"   (At  4)
                       Set "60"   (At  7)
                       Set "null" (At 10)])    

  test.AssertThat (In "minTempRoom1c"
                      [Set "25"   (At  2)
                       Set "null" (At  8)
                       Set "23"   (At  9)
                       Set "null" (At 12)])

  test.AssertThat (In "minTempRoom1d"
                      [Set "25"   (At  2)
                       Set "null" (At  8)
                       Set "23"   (At  9)
                       Set "null" (At 12)])
                       
  test.AssertThat (In "eventsPerRoomg"
                    [SetKey "1" "{ a =   25 }" (At  2)
                     SetKey "3" "{ a =   45 }" (At  4)
                     SetKey "2" "{ a =   50 }" (At  6)
                     SetKey "3" "{ a =   30 }" (At  7)
                     SetKey "1" "{ a = null }" (At  8)
                     SetKey "2" "{ a = null }" (At  9)
                     SetKey "3" "{ a = null }" (At 10)])
                       
  test.AssertThat (In "eventsPerRoomh"
                    [SetKey "1"   "25" (At  2)
                     SetKey "3"   "45" (At  4)
                     SetKey "2"   "50" (At  6)
                     SetKey "3"   "30" (At  7)
                     SetKey "1" "null" (At  8)
                     SetKey "1" "null" (At  9)
                     SetKey "1" "null" (At 10)])


[<TestCase ("streams/select.ez")>]
let test_streamsSelect (test:Test) =
    test.AssertThat (In "temp_readingsX2"
                      [Added 0 "{ temperatureX2 =  60 }" (At  0)
                       Added 3 "{ temperatureX2 =  30 }" (At  3)
                       Added 4 "{ temperatureX2 = 100 }" (At  4)])

    test.AssertThat (In "just10"
                      [Added 0 "{ a = 10 }" (At  0)
                       Added 3 "{ a = 10 }" (At  3)
                       Added 4 "{ a = 10 }" (At  4)])

    test.AssertThat (In "combination"
                      [Added 0 "{ a =  60, b = 10, c = 40 }" (At  0)
                       Added 3 "{ a =  30, b = 10, c = 60 }" (At  3)
                       Added 4 "{ a = 100, b = 10, c = 60 }" (At  4)])
                       
    test.AssertThat (In "complex"
                      [Added 4 "{ a = 100, b = 10, c = 60 }" (At  4)])



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
                      [SetKey "1" "{ a = 5, b = 25, c = 90, d = 115 }" (At  2)
                       SetKey "1" "{ a = 5, b = 25, c = 71, d =  96 }" (At  3)
                       SetKey "1" "{ a = 5, b = 25, c = 81, d = 106 }" (At  4)
                       SetKey "3" "{ a = 5, b = 45, c = 81, d = 126 }" (At  4)
                       SetKey "1" "{ a = 5, b = 25, c = 91, d = 116 }" (At  5)
                       SetKey "3" "{ a = 5, b = 45, c = 91, d = 136 }" (At  5)
                       SetKey "1" "{ a = 5, b = 25, c = 72, d =  97 }" (At  6)
                       SetKey "3" "{ a = 5, b = 45, c = 72, d = 117 }" (At  6)
                       SetKey "2" "{ a = 5, b = 50, c = 72, d = 122 }" (At  6)
                       SetKey "1" "{ a = 5, b = 25, c = 82, d = 107 }" (At  7)
                       SetKey "2" "{ a = 5, b = 50, c = 82, d = 132 }" (At  7)
                       SetKey "3" "{ a = 5, b = 30, c = 82, d = 112 }" (At  7)
                       SetKey "1" "{ a = 5, b = 25, c = 92, d = 117 }" (At  8)
                       SetKey "2" "{ a = 5, b = 50, c = 92, d = 142 }" (At  8)
                       SetKey "3" "{ a = 5, b = 30, c = 92, d = 122 }" (At  8)
                       SetKey "1" "{ a = 5, b = 23, c = 92, d = 115 }" (At  9)])
