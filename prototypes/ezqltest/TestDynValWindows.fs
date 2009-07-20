open Test

[<TestCase ("dynValWindows/create_window.ez")>]
let test_windowsCreation2 (test:Test) =

  test.AssertThat (In "currTemp"
                    [Set "25" (At  2)
                     Set "45" (At  4)
                     Set "25" (At  5)
                     Set "50" (At  6)
                     Set "30" (At  7)
                     Set "23" (At  9)])


  test.AssertThat (In "last3secs"
                    [Added     "25" (At  2)
                     Added     "45" (At  4)
                     Expired "null" (At  5)
                     Added     "25" (At  5)
                     Added     "50" (At  6)
                     Added     "30" (At  7)
                     Expired   "25" (At  7)
                     Expired   "45" (At  8)
                     Added     "23" (At  9)
                     Expired   "25" (At  9)
                     Expired   "50" (At 10)
                     Expired   "30" (At 12)])
                     
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
