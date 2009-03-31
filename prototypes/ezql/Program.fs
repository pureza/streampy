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