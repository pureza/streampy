#light

open System
open System.IO
open Types
open Scheduler
open Extensions.DateTimeExtensions
open System.Text.RegularExpressions

type CSVAdapter(stream, reader:TextReader) =
    let tryParse value =
        match Int32.TryParse(value) with
        | true, r -> VInt r
        | _ -> VString value

    let readColumns =
        let line = reader.ReadLine().Trim()
        let m = Regex.Match(line, "#\s*import\s*\"(.*)\"")
        if m.Success
          then CSVAdapter (stream, new StreamReader (m.Groups.[1].Value)) |> ignore
               []
          else line.Split([|'#'|]).[0].Split [|','|]
                 |> Array.map (fun s -> s.Trim().ToLower())
                 |> Array.to_list
        
    let readLine (columns: string list) =
        let line = (reader.ReadLine().Split([|'#'|])).[0]
        if line.Length = 0 
            then None
            else let values = Array.to_list (line.Split [|','|])
                 let values' = List.map tryParse values
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
        
    static member FromString(stream, string) =
        CSVAdapter (stream, new StringReader (string))
