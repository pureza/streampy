open Test

[<TestCase ("muggy/muggy_simple.ez", 1000)>]
let test_muggySimple (test:Test) =
  test.AssertThat (In "spoiled"
                     [SetKey "1" "1" (At 608)
                      SetKey "2" "2" (At 616)])