#light

#light

open Test

[<TestCase ("dynValWindows/create_window.ez")>]
let test_windowsCreation (test:Test) =

  test.AssertThat (In "currTemp"
                    [Set "25" (At  2)
                     Set "45" (At  4)
                     Set "25" (At  5)
                     Set "50" (At  6)
                     Set "30" (At  7)
                     Set "23" (At  9)])


  test.AssertThat (In "last3secs"
                    [Added   2 "25" (At  2)
                     Added   4 "45" (At  4)
                     Added   5 "25" (At  5)
                     Added   6 "50" (At  6)
                     Added   7 "30" (At  7)
                     Expired 2 "25" (At  7)
                     Expired 4 "45" (At  8)
                     Added   9 "23" (At  9)
                     Expired 5 "25" (At  9)
                     Expired 6 "50" (At 10)
                     Expired 7 "30" (At 12)])
                     
  test.AssertThat (In "sumLast3secs"
                    [Set " 25" (At  2)
                     Set " 70" (At  4)
                     Set " 95" (At  5)
                     Set "145" (At  6)
                     Set "150" (At  7)
                     Set "105" (At  8)
                     Set "103" (At  9)
                     Set " 53" (At 10)
                     Set " 23" (At 12)])

  test.AssertThat (In "x"
                    [Set "35" (At  2)
                     Set "55" (At  4)
                     Set "35" (At  5)
                     Set "60" (At  6)
                     Set "40" (At  7)
                     Set "33" (At  9)])