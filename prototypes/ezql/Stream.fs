#light

module Stream

    open System
    open System.Collections.Generic
 
    type 'a Stream() =
        let triggerAdd, addEvent = Event.create()
        member self.OnAdd = addEvent.Add
        member self.Add(item:'a) = triggerAdd item
        member self.Where(predicate) =
            let result = Stream ()
            self.OnAdd (fun t -> if predicate t then result.Add t)
            result
        member self.Select(projector) =
            let result = Stream ()
            self.OnAdd (fun t -> result.Add (projector t))
            result
                                 

    let print (stream: Stream<'a>) = stream.OnAdd (fun t -> printfn "%A" t)      
    
    (*
    let s = Stream ()
    where s (fun ev -> ev.Price > 10) |> print "price > 10"
    select s (fun ev -> { Timestamp = ev.Timestamp; PriceX2 = ev.Price * 2 }) |> print "price * 2"


    s.add { Timestamp = DateTime.Now; Company = "IBM"; Price = 10 }
    s.add { Timestamp = DateTime.Now; Company = "IBM"; Price = 20 }

    Console.ReadLine() |> ignore        

    *)