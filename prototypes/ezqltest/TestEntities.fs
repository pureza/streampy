#light

open Test

[<TestCase ("entities/misc.ez")>]
let test_entitiesMisc (test:Test) =
(*
  test.AssertThat (In "x"
                    [Set " 25" (At  3)
                     Set " 25" (At  4)
                     Set " 75" (At  6)
                     Set " 25" (At  7)
                     Set " 75" (At  8)
                     Set "105" (At  9)])
  *)                   
  test.AssertThat (In "y"
                    [Set " 25" (At  3)
                     Set " 25" (At  4)
                     Set " 25" (At  6)
                     Set " 25" (At  7)
                     Set "100" (At  8)
                     Set "100" (At  9)])

  test.AssertThat (In "products"
                    [SetKey "1" "{ :product_id = 1, :temperature = 25,
                                   :room_id = 1, :room = { :room_id = 1, :temperature = 25 } }" (At  3)
                     SetKey "2" "{ :product_id = 2, :temperature = 25,
                                   :room_id = 1, :room = { :room_id = 1, :temperature = 25 } }" (At  4)
                     SetKey "2" "{ :product_id = 2, :temperature = 75,
                                   :room_id = 2, :room = { :room_id = 2, :temperature = 50 } }" (At  6)
                     SetKey "3" "{ :product_id = 3, :temperature = 25,
                                   :room_id = 1, :room = { :room_id = 1, :temperature = 25 } }" (At  7)
                     SetKey "1" "{ :product_id = 1, :temperature = 75,
                                   :room_id = 2, :room = { :room_id = 2, :temperature = 50 } }" (At  8)
                     SetKey "2" "{ :product_id = 2, :temperature = 105,
                                   :room_id = 3, :room = { :room_id = 3, :temperature = 30 } }" (At  9)
                     SetKey "3" "{ :product_id = 3, :temperature = 48,
                                   :room_id = 1, :room = { :room_id = 1, :temperature = 23 } }" (At  9)])                                 


[<TestCase ("entities/misc2.ez")>]
let test_entitiesMisc2 (test:Test) = () 