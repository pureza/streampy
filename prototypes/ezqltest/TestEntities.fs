#light

open Test
open Types

[<TestCase ("entities/misc.ez")>]
let test_entitiesMisc (test:Test) =

  test.AssertThat (In "x"
                    [Set " 25" (At  3)
                     Set " 75" (At  6)
                     Set " 25" (At  7)
                     Set " 75" (At  8)
                     Set "105" (At  9)])
                   
  test.AssertThat (In "y"
                    [Set " 25" (At  3)
                     Set "100" (At  8)])

  test.AssertThat (In "products"
                    [SetKey "1" "{ product_id = 1, temperature = 25,
                                   room_id = 1, room = { room_id = 1, temperature = 25 } }" (At  3)
                     SetKey "2" "{ product_id = 2, temperature = 25,
                                   room_id = 1, room = { room_id = 1, temperature = 25 } }" (At  4)
                     SetKey "2" "{ product_id = 2, temperature = 75,
                                   room_id = 2, room = { room_id = 2, temperature = 50 } }" (At  6)
                     SetKey "3" "{ product_id = 3, temperature = 25,
                                   room_id = 1, room = { room_id = 1, temperature = 25 } }" (At  7)
                     SetKey "1" "{ product_id = 1, temperature = 75,
                                   room_id = 2, room = { room_id = 2, temperature = 50 } }" (At  8)
                     SetKey "2" "{ product_id = 2, temperature = 105,
                                   room_id = 3, room = { room_id = 3, temperature = 30 } }" (At  9)
                     SetKey "3" "{ product_id = 3, temperature = 48,
                                   room_id = 1, room = { room_id = 1, temperature = 23 } }" (At  9)])                                 

  let complicatedDiffs =
    [SetKeyRaw "1" (VDict (Map.empty))                                      (At 2)     // Room 1 is created
     SetKeyRaw "1" (VDict ((Map.of_list [VInt 1, VInt 1])))                 (At 3)     // Room 1 contains product 1
     SetKeyRaw "3" (VDict (Map.empty))                                      (At 4)     // Room 3 is created
     SetKeyRaw "1" (VDict ((Map.of_list [VInt 1, VInt 1; VInt 2, VInt 1]))) (At 4)     // Room 1 now contains product 2
     SetKeyRaw "1" (VDict ((Map.of_list [VInt 1, VInt 1])))                 (At 6)     // Product 2 leaves room 1
     SetKeyRaw "2" (VDict ((Map.of_list [VInt 2, VInt 2])))                 (At 6)     // Product 2 enters room 2
     SetKeyRaw "1" (VDict ((Map.of_list [VInt 1, VInt 1; VInt 3, VInt 1]))) (At 7)     // Product 3 enters room 1
     SetKeyRaw "1" (VDict ((Map.of_list [VInt 3, VInt 1])))                 (At 8)     // Product 1 leaves room 1
     SetKeyRaw "2" (VDict ((Map.of_list [VInt 1, VInt 2; VInt 2, VInt 2]))) (At 8)     // Product 1 enters room 2
     SetKeyRaw "2" (VDict ((Map.of_list [VInt 1, VInt 2])))                 (At 9)     // Product 2 leaves room 2
     SetKeyRaw "3" (VDict ((Map.of_list [VInt 2, VInt 3])))                 (At 9)]    // Product 2 enters room 3

  test.AssertThat (In "roomsPerProducts" complicatedDiffs)
  test.AssertThat (In "roomsPerProducts2" complicatedDiffs)
  
  test.AssertThat (In "temp2SecsPerRoom"
                     [SetKey "1" "25" (At  2)
                      SetKey "3" "45" (At  4)
                      SetKey "2" "50" (At  6)
                      SetKey "3" "75" (At  7)
                      SetKey "1" "48" (At  9)
                      SetKey "3" "30" (At  9)
                      SetKey "1" "23" (At 11)])


[<TestCase ("entities/misc3.ez")>]
let test_entitiesMisc3 (test:Test) =
  test.AssertThat (In "productIdsPerRoom"
                    [SetKeyRaw "1" (VDict (Map.empty))                                      (At 2)
                     SetKeyRaw "1" (VDict ((Map.of_list [VInt 1, VInt 1])))                 (At 3)     // Room 1 is created
                     SetKeyRaw "3" (VDict (Map.empty))                                      (At 4)     // Room 3 is created
                     SetKeyRaw "1" (VDict ((Map.of_list [VInt 1, VInt 1; VInt 2, VInt 2]))) (At 4)     // Room 1 now contains product 2
                     SetKeyRaw "1" (VDict ((Map.of_list [VInt 1, VInt 1])))                 (At 6)     // Product 2 leaves room 1
                     SetKeyRaw "2" (VDict ((Map.of_list [VInt 2, VInt 2])))                 (At 6)     // Product 2 enters room 2
                     SetKeyRaw "1" (VDict ((Map.of_list [VInt 1, VInt 1; VInt 3, VInt 3]))) (At 7)     // Product 3 enters room 1
                     SetKeyRaw "1" (VDict ((Map.of_list [VInt 3, VInt 3])))                 (At 8)     // Product 1 leaves room 1
                     SetKeyRaw "2" (VDict ((Map.of_list [VInt 1, VInt 1; VInt 2, VInt 2]))) (At 8)     // Product 1 enters room 2
                     SetKeyRaw "2" (VDict ((Map.of_list [VInt 1, VInt 1])))                 (At 9)     // Product 2 leaves room 2
                     SetKeyRaw "3" (VDict ((Map.of_list [VInt 2, VInt 2])))                 (At 9)])  

  test.AssertThat (In "totalProductsPerRoom"
                     [SetKey "1" "0" (At  2)
                      SetKey "1" "1" (At  3)
                      SetKey "3" "0" (At  4)
                      SetKey "1" "2" (At  4)
                      SetKey "1" "1" (At  6)
                      SetKey "2" "1" (At  6)
                      SetKey "1" "2" (At  7)
                      SetKey "1" "1" (At  8)
                      SetKey "2" "2" (At  8)
                      SetKey "2" "1" (At  9)
                      SetKey "3" "1" (At  9)])


[<TestCase ("entities/misc2.ez", 1000)>]
let test_entitiesMisc2 (test:Test) = ()

                                                                                             
                    