#light

open Test

[<TestCase ("entities/misc.ez")>]
let test_entitiesMisc (test:Test) =
  test.AssertThat (In "x"
                    [Set " 25" (At  3)
                     Set " 25" (At  4)
                     Set " 75" (At  6)
                     Set " 25" (At  7)
                     Set " 75" (At  8)
                     Set "105" (At  9)])
                     
  test.AssertThat (In "y"
                    [Set " 25" (At  3)
                     Set " 25" (At  4)
                     Set " 25" (At  6)
                     Set " 25" (At  7)
                     Set "100" (At  8)
                     Set "100" (At  9)])

  test.AssertThat (In "$Product_all"
                    [SetKey "1" "{ :timestamp = 3, :product_id = 1, :temperature = 25,
                                   :room_id = 1, :room = { :timestamp = 2, :room_id = 1, :temperature = 25 } }" (At  3)
                     SetKey "2" "{ :timestamp = 4, :product_id = 2, :temperature = 25,
                                   :room_id = 1, :room = { :timestamp = 2, :room_id = 1, :temperature = 25 } }" (At  4)
                     SetKey "2" "{ :timestamp = 6, :product_id = 2, :temperature = 75,
                                   :room_id = 2, :room = { :timestamp = 6, :room_id = 2, :temperature = 50 } }" (At  6)
                     SetKey "3" "{ :timestamp = 7, :product_id = 3, :temperature = 25,
                                   :room_id = 1, :room = { :timestamp = 5, :room_id = 1, :temperature = 25 } }" (At  7)
                     SetKey "1" "{ :timestamp = 8, :product_id = 1, :temperature = 75,
                                   :room_id = 2, :room = { :timestamp = 6, :room_id = 2, :temperature = 50 } }" (At  8)
                     SetKey "2" "{ :timestamp = 9, :product_id = 2, :temperature = 105,
                                   :room_id = 3, :room = { :timestamp = 7, :room_id = 3, :temperature = 30 } }" (At  9)
                     SetKey "3" "{ :timestamp = 7, :product_id = 3, :temperature = 48,
                                   :room_id = 1, :room = { :timestamp = 9, :room_id = 1, :temperature = 23 } }" (At  9)])

[<TestCase ("entities/misc2.ez")>]
let test_entitiesMisc2 (test:Test) = ()                                                                    