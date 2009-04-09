#light

open System
open Types
open Oper
open MapOper

(*
          

let tempReadings = stream 1.0 "temp_readings"
let wher = where (fun ev -> ev.Timestamp > DateTime.Now) 1.0 "where"
let hotReadings = stream 3.0 "hotReadings"
let currTemp = last "temp" 4.0 "currTemp"
let currTempX2 = add 7.0 "currTempX2" 
let blah = stream 5.0 "blah"
let lastBlah = last "ignore" 6.0 "lastBlah"
   

connect tempReadings wher
connect wher hotReadings
connect tempReadings currTemp
connect blah lastBlah
connect currTemp currTempX2
connect currTemp currTempX2




//blah.Value <- VInt 50

let ev = event (DateTime.Now, Map.of_list [("temp", VInt 30)])
spread [(tempReadings, [(0, [Added (VEvent ev)])])]

*)


(*
 * The following graph tries to mimic this code:
 *
 * a = temp_readings.last(:temp)
 *
 * tempPerRoom = temp_readings
 *                 .groupby(:room_id, g -> g.last(:temp) + a)
 *
 * hotRooms = tempPerRoom
 *              .where(t -> t > 30)
 *)



(*
let tempReadings = stream 1.0 "temp_readings"
let blah = last "temp" 2.0 "lastBlah"
let tempPerRoom = groupby "room_id" 3.0 
                          (fun parentPrio -> let fixPrio n = parentPrio + (n * 0.01)
                                             let g = stream (fixPrio 1.0) "sub:g"
                                             let gLast = last "temp" (fixPrio 2.0) "sub:currTemp"
                                             let gAdd = adder (fixPrio 3.0) "+"
                                             connect g gLast id
                                             connect gLast gAdd id
                                             connect blah gAdd id
                                             g)

let hotRooms = mapWhere 4.0 
                        (fun parentPrio ->
                           let fixPrio n = parentPrio + (n * 0.01)
                           let biggerThan = unaryOp (fun op changes -> match changes with
                                                                       | [Added (VInt t)] -> setValueAndGetChanges op (VBool (t > 30))
                                                                       | _ -> failwithf "Invalid input for biggerTan: %A" changes)
                                                    VNull (fixPrio 1.0) "sub:>"
                           biggerThan)

let tempPerRoomDoubled = mapSelect 5.0
                                   (fun parentPrio ->
                                      let fixPrio n = parentPrio + (n * 0.01)
                                      let doubleTemp = unaryOp (fun op changes -> match changes with
                                                                                  | [Added (VInt t)] -> setValueAndGetChanges op (VInt (t * 2))
                                                                                  | _ -> failwithf "Invalid input for doubleTemp: %A" changes)
                                                               VNull (fixPrio 1.0) "sub:*2"
                                      doubleTemp)

let dummy = unaryOp (fun _ _ -> None) VNull 6.0 "dummy"
let record = makeRecord [|("blah", blah); ("dummy", dummy)|] 7.0 "record"

connect tempReadings tempPerRoom id
connect tempReadings blah id
connect tempPerRoom hotRooms id
connect hotRooms tempPerRoomDoubled id

let evs = [event (DateTime.Now, Map.of_list [("room_id", VInt 1); ("temp", VInt 30)])
           event (DateTime.Now, Map.of_list [("room_id", VInt 2); ("temp", VInt 40)])
           event (DateTime.Now, Map.of_list [("room_id", VInt 2); ("temp", VInt 10)])]
           
for ev in evs do
  spread [(tempReadings, ([(0, [Added (VEvent ev)])]))] |> ignore
  printfn "tempPerRoom: %A" tempPerRoom.Value
  printfn "hotRooms: %A" hotRooms.Value
  printfn "tempPerRoomDoubled: %A" tempPerRoomDoubled.Value
  printfn "record: %A" record.Value
  printfn ""

Console.ReadLine() |> ignore

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

open Ast
open Graph
open Dataflow


let streams, allOps = Engine.compile @"
                            temp_readings = stream (:room_id, :temperature);

                            x = temp_readings.last(:room_id);
                            y = temp_readings.last(:temperature);
                            
                            //z = x + y * 3;
                            hot_readings = temp_readings.where(ev -> ev.temperature > x);
                            
                            lastHot = hot_readings.last(:temperature);
                            
                            //a = (y - x + (x * temp_readings.last(:temperature)));
                            
                            //y = temp_readings.last(:temperature);
                            
                            //z = y * (x + x[3 min].max());
                            
                            //tempPerRoom = temp_readings.groupby(:room_id, g -> g.last(:temperature) > x);
                           "


streams.["temp_readings"] (Event (DateTime.Now, Map.of_list [("room_id", VInt 1); ("temperature", VInt 30)]))
streams.["temp_readings"] (Event (DateTime.Now, Map.of_list [("room_id", VInt 1); ("temperature", VInt 20)]))

printfn "%A" allOps.["lastHot"].Value

(*

let expr = Parser.expr Lexer.token (Lexing.from_string "temp_readings[5 min].last(:temp) + x * y")

printfn "%A" expr

let temp_readings = { Uid = "temp_readings"; Type = Stream; Name = "temp_readings"; MakeOper = fun uid prio -> failwith "not" }
let x = { Uid = "x"; Type = DynVal; Name = "x"; MakeOper = fun uid prio -> failwith "not" }
let y = { Uid = "y"; Type = DynVal; Name = "y"; MakeOper = fun uid prio -> failwith "not" }

let graph = Graph.empty()
              |> Graph.add ([], "x", x, [])
              |> Graph.add ([], "y", y, [])
              |> Graph.add ([], "temp_readings", temp_readings, [])
              
let env = Map.of_list [("x", x); ("y", y); ("temp_readings", temp_readings)]

let deps, graph', expr' = dataflowE env graph expr

printfn "%A" (Set.map (fun n -> n.Uid) deps)
printfn "%A" expr'
*)
Console.ReadLine() |> ignore