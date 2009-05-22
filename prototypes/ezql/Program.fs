(*
let parse code =
    // let lexbuf = Lexing.from_text_reader Encoding.ASCII file
    let lexbuf = Lexing.from_string code
    try
        Parser.toplevel Lexer.token lexbuf
    with e ->
        let pos = lexbuf.EndPos
        failwithf "Error near line %d, character %d\n" (pos.Line + 1) pos.Column

printfn "%A" (parse 
"
x = temp_readings
      .groupby(:room_id, g -> g.last(:temperature))
      .where (t -> t > 25);;

y = temp_readings.where(ev -> ev.temperature > 20;
                              print(ev));;

when (x.updated(),
      ev -> let a = x * x in
            if true then print (x) else print (a));;
")

//when(a.updated(), a -> print(x + y));
//print(3 + 4);
*)
(*
open Graph

let testTriangleGraph () =
  let g = Graph.empty()
            |> Graph.add ([], 1, "A", [])
            |> Graph.add ([1], 2, "B", [])
            |> Graph.add ([1; 2], 3, "C", [])
  printfn "%A" ((Graph.Algorithms.topSort [1] g) = [1; 2; 3])

  let h = Graph.empty()
            |> Graph.add ([], 1, "A", [])
            |> Graph.add ([1], 3, "C", [])
            |> Graph.add ([1], 2, "B", [3])

  printfn "%A" ((Graph.Algorithms.topSort [1] h) = [1; 2; 3])

let testGraphNetworkX () =
    let g = Graph.empty()
              |> Graph.add ([], 1, "A", [])
              |> Graph.add ([1], 2, "2", [])
              |> Graph.add ([1], 3, "3", [])
              |> Graph.add ([1], 4, "4", [])
              |> Graph.add ([2], 5, "5", [])
              |> Graph.add ([2], 6, "6", [])
              |> Graph.add ([2], 7, "7", [])
              |> Graph.add ([2], 8, "8", [])
              |> Graph.add ([6], 9, "9", [])
              |> Graph.add ([6], 10, "10", [])
              |> Graph.add ([6], 11, "12", [])
              |> Graph.add ([4], 12, "12", [])
              |> Graph.add ([4], 13, "13", [])
              |> Graph.add ([4], 14, "14", [])
    printfn "%A" ((Graph.Algorithms.topSort [1] g) = [1; 4; 14; 13; 12; 3; 2; 8; 7; 6; 11; 10; 9; 5])

    let h = g |> Graph.add ([7], 15, "15", [8])
    Graph.Viewer.display h
    printfn "%A" ((Graph.Algorithms.topSort [1] h) = [1; 4; 14; 13; 12; 3; 2; 7; 15; 8; 6; 11; 10; 9; 5])

testGraphNetworkX ()
testTriangleGraph ()

*)
