#light

open System
open System.IO
open Types
open Scheduler

type IAdapter =
    abstract OnEvent : (Event -> unit)

type CSVAdapter(stream:IStream, reader:TextReader) =
    let tryParse value =
        match Int32.TryParse(value) with
        | s, r when s -> VInteger r
        | _ -> failwith "Unrecognized type"

    let readColumns =
        let line = reader.ReadLine()       
        line.Split [|','|]
        |> Array.map (fun s -> s.Trim())
        |> Array.to_list
        |> List.tl
        
    let readLine (columns: string list) =
        let line = reader.ReadLine()
        let timestamp, values =
            match Array.to_list (line.Split [|','|]) with
            | timestamp::values when values.Length = columns.Length -> (Int64.Parse(timestamp)), values
            | _ -> failwith "error"
        let values' = List.map tryParse values
        let fields = Map.of_list (List.zip columns values')
        Event(DateTime.MinValue.AddSeconds(float(timestamp)), fields) :> IEvent
        
    let read(reader) =
        seq { let columns = readColumns
              while (reader :> TextReader).Peek() <> -1 do
                  yield readLine columns }
    
    let events = read(reader)
    do for ev in events do
           Scheduler.schedule ev.Timestamp
                              (fun _ -> stream.Add(ev))
        
    static member FromString(stream, string) =
        CSVAdapter (stream, new StringReader (string))