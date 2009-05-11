#light

open Test

[<TestCase ("entities/misc.ez")>]
let test_misc (test:Test) =
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