open System
open System.IO
open Types
open Scheduler
open Extensions.DateTimeExtensions
open System.Text.RegularExpressions
open Ast

type CSVAdapter(stream, fields:Map<string, Type>, reader:TextReader) =
    let tryParse column value =
        let parser = match fields.[column] with
                     | TyInt -> Int32.Parse >> VInt
                     | TyFloat -> Single.Parse >> VFloat
                     | TyBool -> Boolean.Parse >> VBool
                     | _ -> id >> VString
        parser value

    let readColumns =
        let line = reader.ReadLine().Trim()
        let m = Regex.Match(line, "#\s*import\s*\"(.*)\"")
        if m.Success
          then CSVAdapter (stream, fields, new StreamReader (m.Groups.[1].Value)) |> ignore
               []
          else line.Split([|'#'|]).[0].Split [|','|]
                 |> Array.map (fun s -> s.Trim())
                 |> Array.to_list

    let readLine (columns: string list) =
        let line = (reader.ReadLine().Split([|'#'|])).[0]
        if line.Length = 0
            then None
            else let values = Array.to_list (line.Split [|','|])
                 let values' = List.map2 tryParse columns values
                 let fields = Map.of_list (List.zip (List.map VString columns) values')
                 Some (VRecord fields)

    let read(reader) =
        seq { let columns = readColumns
              while (reader :> TextReader).Peek() <> -1 do
                  yield readLine columns }

    let events = read(reader)
    do for ev in events do
           match ev with
           | Some (VRecord fields as ev')  ->
               let timestamp = match fields.[VString "timestamp"] with
                               | VInt t -> DateTime.FromSeconds(t)
                               | _ -> failwithf "timestamp is not an integer"
               Scheduler.schedule timestamp ([stream, 0, id], [Added ev'])
           | _ -> ()

    static member FromString(stream, fields, string) =
        CSVAdapter (stream, fields, new StringReader (string))
