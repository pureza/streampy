#light

open System
open System.IO
open Types
open Scheduler

type IAdapter =
    abstract OnEvent : (Event -> unit)


type CSVAdapter(stream, reader:TextReader) =
    let tryParse value =
        match Int32.TryParse(value) with
        | s, r when s -> VInt r
        | _ -> failwith "Unrecognized type"

    let readColumns =
        let line = (reader.ReadLine().Split([|'#'|])).[0] 
        line.Split [|','|]
          |> Array.map (fun s -> s.Trim())
          |> Array.to_list
          |> List.tl
        
    let readLine (columns: string list) =
        let line = (reader.ReadLine().Split([|'#'|])).[0]
        if line.Length = 0 
            then None
            else let timestamp, values =
                     match Array.to_list (line.Split [|','|]) with
                     | timestamp::values when values.Length = columns.Length -> (Int64.Parse(timestamp)), values
                     | _ -> failwith "error"
                 let values' = List.map tryParse values
                 let fields = Map.of_list (List.zip columns values')
                 Some (Event(DateTime.MinValue.AddSeconds(float(timestamp)), fields))
        
    let read(reader) =
        seq { let columns = readColumns
              while (reader :> TextReader).Peek() <> -1 do
                  yield readLine columns }
    
    let events = read(reader)
    do for ev in events do
           match ev with
           | Some event -> Scheduler.schedule event.Timestamp (stream, Added (VEvent event))
           | _ -> ()
        
    static member FromString(stream, string) =
        CSVAdapter (stream, new StringReader (string))
