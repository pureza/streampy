#light

open System
open System.Timers

type IClock =
    abstract Schedule : DateTime * (unit -> unit) -> unit
    abstract Now : DateTime

    
type VirtualClock() =
    let mutable now = DateTime.MinValue
    let mutable nextEvent = None
    
    interface IClock with
        member self.Schedule(time, action) = nextEvent <- Some (time, action)
        member self.Now = now
        
    member self.Step () =
        match nextEvent with
        | Some (time, action) -> nextEvent <- None
                                 now <- time
                                 action()
        | None -> failwithf "nextEvent was None"
        
    member self.HasNext () =
        match nextEvent with
        | Some _ -> true
        | None -> false

type RealClock() =
    let mutable nextAction = None
    let timer = new Timer()
    
    do timer.Elapsed.Add (fun _ -> timer.Enabled <- false
                                   match nextAction with
                                   | Some action -> nextAction <- None
                                                    action()
                                   | None -> failwith "No action!")
    interface IClock with
        member self.Schedule(time, action) =
            nextAction <- Some action
            timer.Interval <- float((time - DateTime.Now).TotalMilliseconds)
            timer.Enabled <- true                                           
        member self.Now = DateTime.Now  