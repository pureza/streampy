open Test

[<TestCase ("functions/functions.ez")>]
let test_functions (test:Test) =
  test.AssertThat (In "a"
                    [Set " 50" (At  2)
                     Set " 90" (At  4)
                     Set " 50" (At  5)
                     Set "100" (At  6)
                     Set " 60" (At  7)
                     Set " 46" (At  9)])

  test.AssertThat (In "b"
                    [Set " 25" (At  2)
                     Set " 70" (At  4)
                     Set " 95" (At  5)
                     Set "120" (At  6)
                     Set "105" (At  7)
                     Set " 80" (At  8)
                     Set " 53" (At  9)
                     Set " 23" (At 11)])
                     
  test.AssertThat (In "c"
                    [Set "25" (At  2)
                     Set "45" (At  4)
                     Set "25" (At  5)
                     Set "50" (At  6)
                     Set "30" (At  7)
                     Set "23" (At  9)])                      

  test.AssertThat (In "d"
                    [Set "26" (At  2)
                     Set "46" (At  4)
                     Set "26" (At  5)
                     Set "51" (At  6)
                     Set "31" (At  7)
                     Set "24" (At  9)])    
 
  test.AssertThat (In "e"
                    [Set " 350" (At  2)
                     Set "1080" (At  4)
                     Set " 350" (At  5)
                     Set "1325" (At  6)
                     Set " 495" (At  7)
                     Set " 299" (At  9)])  
                     
  test.AssertThat (In "f"
                    [Set " 50" (At  2)
                     Set "115" (At  4)
                     Set "120" (At  5)
                     Set "195" (At  6)
                     Set "205" (At  7)
                     Set "221" (At  9)])  

  test.AssertThat (In "g"
                    [Set " 350" (At  2)
                     Set "1080" (At  4)
                     Set " 350" (At  5)
                     Set "1325" (At  6)
                     Set " 495" (At  7)
                     Set " 299" (At  9)])        
                     
  test.AssertThat (In "h"
                    [Set "  25" (At  2)
                     Set "1080" (At  4)
                     Set "  50" (At  5)
                     Set "1325" (At  6)
                     Set " 495" (At  7)
                     Set "  73" (At  9)])                                                                                                                          
