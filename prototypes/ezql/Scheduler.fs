#light

open System
open System.Collections.Generic
open Clock

type callback = (unit -> unit)

type Scheduler =
    { clock:IClock
      queue:SortedList<DateTime, callback list> }

let init () =
    { clock = VirtualClock ()
      queue = SortedList<DateTime, callback list>() }

let sched = ref (init ())

let reset () = sched := init ()

let clock () = (!sched).clock

let rec reSchedule sched =
    let queue = sched.queue
    if queue.Count <> 0
        then sched.clock.Schedule(queue.Keys.[0], 
                                  fun _ -> List.iter (fun action -> action()) queue.Values.[0]
                                           queue.RemoveAt 0
                                           reSchedule sched)
                                           
let schedule time action =
    let queue = (!sched).queue
    if queue.ContainsKey(time)
        then queue.[time] <- queue.[time] @ [action]
        else queue.[time] <- [action]
    reSchedule (!sched)

let scheduleOffset offset action =
    schedule ((!sched).clock.Now + TimeSpan(0, 0, offset)) action
    
