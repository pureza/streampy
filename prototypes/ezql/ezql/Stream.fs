#light

module EzQL.Stream

    open System
    open System.Collections.Generic

(*
    type Stocks = { Timestamp:DateTime
                    Company:string
                    Price:int }   
                    
    type StocksX2 = { Timestamp:DateTime
                      PriceX2:int }                 
*)    
    type 'a Stream() =
        let triggerUpdate, update = Event.create()
        member self.OnUpdate = update.Add
        member self.add (item:'a) = triggerUpdate item

    let empty<'a> = Stream<'a> ()
           
    let where (stream: Stream<'a>) predicate =
        let result = Stream ()
        stream.OnUpdate (fun t -> if predicate t then result.add t)
        result
        
    let select (stream: Stream<'a>) projector =
        let result = Stream ()
        stream.OnUpdate (fun t -> result.add (projector t))
        result

    let print str (stream: Stream<'a>) = stream.OnUpdate (fun t -> printfn "(%s) -> %A" str t)      
    
    (*
    let s = Stream ()
    where s (fun ev -> ev.Price > 10) |> print "price > 10"
    select s (fun ev -> { Timestamp = ev.Timestamp; PriceX2 = ev.Price * 2 }) |> print "price * 2"


    s.add { Timestamp = DateTime.Now; Company = "IBM"; Price = 10 }
    s.add { Timestamp = DateTime.Now; Company = "IBM"; Price = 20 }

    Console.ReadLine() |> ignore        

    *)