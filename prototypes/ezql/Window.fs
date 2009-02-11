#light

#light

open System
open System.Collections.Generic
open Stream

type 'a Window(interval) =
    let triggerAdd, addEvent = Event.create()
    let triggerExpire, expireEvent = Event.create()
    member self.OnAdd = addEvent.Add
    member self.OnExpire (action:'a -> unit) = expireEvent.Add action
    member self.Add (item:'a) =
        match interval with
        | None -> ()
        | Some value -> EventQueue.instance.Register(DateTime.Now.AddSeconds(float value),
                                                    fun () -> triggerExpire item)
        triggerAdd item
    member self.Expire = triggerExpire
    
    member self.Where(predicate) =
        let result = Window (None)
        self.OnAdd (fun t -> if predicate t then result.Add t)
        self.OnExpire (fun t -> if predicate t then result.Expire t)
        result
    
    member self.Select(projector) =
        let result = Window (None)
        self.OnAdd (fun t -> result.Add (projector t))
        self.OnExpire (fun t -> result.Expire (projector t))
        result
        
    static member FromStream(stream: 'a Stream, interval) =
        let result = Window(Some interval)
        stream.OnAdd (fun ev -> result.Add ev)
        result

let print (window: Window<'a>) =
    window.OnAdd (fun t -> printfn "+ %A" t) 
    window.OnExpire (fun t -> printfn "- %A" t) 