#light

open System
open System.Collections.Generic

type event(timestamp:DateTime, fields:Map<string, value>) =
    member self.Timestamp with get() = timestamp
    member self.Item with get(field) = fields.[field]
    member self.Fields = fields

and value =
    | VBool of bool
    | VInt of int
    | VDict of Dictionary<value, value>
    | VEvent of event
    | VNull
    
    override self.ToString() =
      let s = match self with
              | VBool b -> b.ToString()
              | VInt v -> v.ToString()
              | VDict m -> m.ToString()
              | VEvent ev -> ev.ToString()
              | VNull -> "VNull"
      (sprintf "« %s »" s)
    
    static member (+)(left:value, right:value) =
        match left, right with
        | VInt l, VInt r -> VInt (l + r)
        | _ -> failwith "Invalid types in +"

type diff =
    | Added of value
    | Expired of value
    | DictDiff of value * diff list
    | RemovedKey of value

type changes = diff list
type priority = float
type link = changes -> changes

(*
 * An operator is like a graph node that contains a list of parents and
 * children.
 *
 * An operator knows what to do when it is evaluated with a list of inputs.
 * This evaluation function is called with the following arguments:
 * - op: the operator itself ("this")
 * - list<diff list>: lists the differences for each input (for instance, if
 *                    the first input is a window and the second a value,
 *                    this list could be [[AddedEvent ev1; ExpiredEvent ev2]
 *                                        [Set (VInt 3)]]
 *                    If the eval needs the value of the argument and not just
 *                    the diff, it can obtain it through ParentValue(idx)
 *
 * The evaluation function returns:
 * - What changed during this step of the evaluation (a diff list)
 * - The list of (children, inputIndex) to spread these changes
 *)
[<ReferenceEquality>]
type oper = { Eval: oper -> changes list -> (childData List * changes) option
              Children: List<childData>
              Parents: List<oper>
              Contents: ref<value>
              Priority: priority
              Name: string }
  
            member self.ArgCount with get = self.Parents.Count
            member self.Value
                with get = !self.Contents
                and set(v) = self.Contents := v
                
            interface IComparable with 
              member self.CompareTo(other) = Int32.compare (self.GetHashCode()) (other.GetHashCode())
            static member NameOf(op) = op.Name

and childData = oper * int * link

(*
 * Connect parent with child.
 * fn transforms the output of oper before it is delivered to child.
 *)
let connect (parent:oper) (child:oper) fn =
    let index = child.Parents.Count
    
    // parent is the ith input of child
    parent.Children.Add(child, index, fn)
    
    // child knows who is its ith parent
    child.Parents.Insert(index, parent)


// Changes the current value of a continuous value if the new value differs
// from the current one. Also gets the list of changes to propagate.
let setValueAndGetChanges (op:oper) v =
    if v <> op.Value
      then op.Value <- v
           Some (op.Children, [Added v])
      else None


// Example eval stack:
// b = a * c
//
// [b, [(0, [Set (VInt 5)])
//      (1, [])]
//
// This means operator b is to be evaluated because input 0 (could be either
// 'a' or 'c') was set to 5, while the second input stayed the same.

type evalStack = (oper * (int * changes) list) list

(*
 * This is where any change is propagated throughout the graph.
 *)
let rec spread (stack:evalStack) =
    // Joins two evaluation stacks and sorts them by node priority
    let mergeStack (stack:evalStack) (toMerge:evalStack) : evalStack =
        let sortWithOrder = Map.to_list >> List.sort_by (fun (op, _) -> op.Priority) 

        let stackMap = Map.of_list stack
        let merged = List.fold_left (fun acc (k, v) ->
                                       if Map.mem k acc
                                         then Map.add k (acc.[k]@v) acc
                                         else Map.add k v acc) stackMap toMerge
        merged |> sortWithOrder 
        
    let rec fillLeftArgs (op:oper) inputs idx : changes list =
        match (List.sort_by fst inputs) with
        | (next, changes)::xs -> // If there were no changes for input idx, cons []
                                 if idx < next
                                   then []::(fillLeftArgs op inputs (idx + 1))
                                   else changes::(fillLeftArgs op xs (idx + 1))
        // If there are no more changes but there are still arguments to be filled...
        | [] when idx < op.ArgCount -> [ for i in [1 .. (op.ArgCount - idx)] -> [] ]
        | _ -> []
        
    match stack with
    | [] -> ()
    | (op, parentChanges)::xs -> 
        let filledChanges = fillLeftArgs op parentChanges 0
        match op.Eval op filledChanges with
        | Some (children, changes) -> spread (mergeStack xs [ for child, idx, link in children -> (child, [(idx, (link changes))]) ])
        | _ -> spread xs


// Some common operators

let unaryOp receiver initial prio name = 
  { Eval = (fun oper inputs -> match inputs with
                               | [v] -> receiver oper v
                               | _ -> failwith "Wrong number of arguments!")
    Children = List<_> ()
    Parents = List<_> ()
    Contents = ref initial
    Priority = prio
    Name = name }
                    
let binaryOp receiver initial prio name =
  { Eval = (fun oper inputs -> match inputs with
                               | [right; left] -> receiver oper right left
                               | _ -> failwith "Wrong number of arguments!")
    Children = List<_> ()
    Parents = List<_> ()
    Contents = ref initial
    Priority = prio
    Name = name }

let stream prio name =
    unaryOp (fun op changes -> match changes with
                               | [Added (VEvent ev)] -> Some (op.Children, changes)
                               | _ -> failwithf "Invalid input: %A" changes)
            VNull prio name


let where predicate prio name =
    unaryOp (fun op changes -> match changes with
                               | [Added (VEvent ev)] -> if predicate ev then Some (op.Children, changes) else None
                               | _ -> failwithf "Invalid input: %A" changes)
            VNull prio name
                              

let last field prio name =
    unaryOp (fun op changes -> match changes with
                               | [Added (VEvent ev)] -> setValueAndGetChanges op ev.[field]
                               | _ -> failwithf "Invalid input: %A" changes)
            VNull prio name

let adder pri name =
    binaryOp (fun op rightChg leftChg -> let v = op.Parents.[0].Value + op.Parents.[1].Value
                                         setValueAndGetChanges op v)
             VNull pri name


let groupby field prio groupBuilder =
    let substreams = ref Map.empty
    let results = new Dictionary<value, value>()
    
    // Returns the last node of an expression (the one that contains the result)
    let rec followCircuit (op:oper) =
        if op.Children.Count = 0
          then op
          else let child, _, _ = op.Children.[0]
               followCircuit child
    
    let rec buildSubGroup key = 
      let group = groupBuilder prio
      let result = followCircuit group
      substreams := (!substreams).Add(key, group)
      connect groupOp group id
      // Connects the resulting node to the dictionary operator.
      // The diff is converted into a DictDiff before being passed to the child.
      connect result dictOp (fun changes -> [DictDiff (key, changes)])
    
    and groupOp = 
      { Eval = (fun op changes ->
                  match changes.Head with
                  | [Added (VEvent ev)] -> let key = ev.[field]
                                           if not (Map.mem key !substreams)
                                             then buildSubGroup key

                                           let group = (!substreams).[key]
                                           Some (List<_>([group, 0, id]), changes.Head)
                  | other -> failwithf "Invalid arguments for groupby! %A" other)
        Children = List<_> ()
        Parents = List<_> ()
        Contents = ref VNull
        Priority = prio
        Name = "groupby" }
        
    and dictOp =
      { Eval = (fun op changes -> 
                  List.iteri (fun i chg -> 
                                match chg with
                                | [] -> ()
                                | [DictDiff (key, _)] -> results.[key] <- op.Parents.[i].Value // TODO remove values that didn't change
                                | _ -> failwithf "Dictionary expects all diffs to be of type DictDiff but received %A" chg)
                              changes
                  Some (op.Children, List.map_concat id changes))
        Children = List<_> ()
        Parents = List<_> ()
        Contents = ref (VDict results)
        Priority = prio + 0.9
        Name = "groupby_dict" }
         
    { groupOp with 
        Children = dictOp.Children;
        Contents = dictOp.Contents }


(*
 * Structure:
 *
 * - whereOp receives the changes from the parent dictionary and propagates
 *   them to the right predicate circuits;
 * - whereOp also sends these changes to dictOp;
 * - dictOp receives the changes from whereOp and the changes from each
 *   predicate and updates the results dictionary accordingly.
 *)  
let mapWhere prio predicateBuilder =
    let predicates = ref Map.empty
    let results = Dictionary<value, value>()
    
    // Returns the last node of an expression (the one that contains the result)
    let rec followCircuit (op:oper) =
        if op.Children.Count = 0
          then op
          else let child, _, _ = op.Children.[0]
               followCircuit child
    
    let rec buildPredCircuit key = 
      let circuit = predicateBuilder prio
      let result = followCircuit circuit
      predicates := (!predicates).Add(key, circuit)

      // Connects the resulting node to the dictionary operator.
      // The diff is converted into a DictDiff before being passed to the child.
      connect result dictOp (fun changes -> [DictDiff (key, changes)])
    
    and whereOp = 
      { Eval = (fun op changes ->
                    let prntChg = changes.Head
                    let predsToEval = prntChg
                                        |> List.map (function
                                                     | DictDiff (key, diff) -> 
                                                         if not (Map.mem key !predicates) then buildPredCircuit key
                                                           
                                                         // The link between the where and the group's circuit ignores everything
                                                         // that is not related to that group.
                                                         let link = (fun changes -> [ for diff in changes do
                                                                                        match diff with
                                                                                        | DictDiff (key', v) when key = key' -> yield! v
                                                                                        | _ -> () ])
                                                         (!predicates).[key], 0, link
                                                     | _ -> failwith "Map/where received invalid changes")
                    Some (List<_> ((dictOp, 0, id)::predsToEval), prntChg))            
        Children = List<_> ()
        Parents = List<_> ()
        Contents = ref VNull
        Priority = prio
        Name = "where" }
    
    and dictOp =
      { Eval = (fun op changes ->
                  // changes.Head contains the dictionary changes passed to the where
                  // changes.Tail contains the changes in the inner predicates
                  let prntChg, predChg = changes.Head, changes.Tail     
                  let parentDict = match whereOp.Parents.[0].Value with
                                   | VDict d -> d
                                   | _ -> failwith "The parent of this where is not a dictionary!"         
                  
                  // Update the results dictionary and collect removed keys
                  let deletions =                 
                    predChg
                      |> List.map_concat (fun chg -> 
                                            match chg with
                                            | [] -> []
                                            | [DictDiff (key, v)] -> match v with
                                                                     | [Added (VBool true)] -> results.[key] <- parentDict.[key]; []
                                                                     | [Added (VBool false)] -> results.Remove(key) |> ignore; [RemovedKey key]
                                                                     | _ -> failwith "Map's where expects only added vbool changes."
                                            | _ -> failwithf "Dictionary expects all diffs to be of type DictDiff but received %A" chg)
                  
                  // Ignore changes to keys that are filtered out                          
                  let containedChanges = prntChg
                                           |> List.filter (function | DictDiff (key, v) when results.ContainsKey(key) -> true
                                                                    | _ -> false)
                  Some (op.Children, deletions@containedChanges))
        Children = List<_> ()
        Parents = List<_> ()
        Contents = ref (VDict results)
        Priority = prio + 0.9
        Name = "groupby_dict" }
         
    { whereOp with 
        Children = dictOp.Children;
        Contents = dictOp.Contents }    



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
                                                                       | [Added (VInt t)] -> op.Value <- VBool (t > 30) // Propagate even if the value didn't change.
                                                                                             Some (op.Children, [Added op.Value])
                                                                       | _ -> failwithf "Invalid input for biggerTan: %A" changes)
                                                    VNull (fixPrio 1.0) "sub:>"
                           biggerThan)
            

connect tempReadings tempPerRoom id
connect tempReadings blah id
connect tempPerRoom hotRooms id

let ev = event (DateTime.Now, Map.of_list [("room_id", VInt 1); ("temp", VInt 30)])
spread [(tempReadings, ([(0, [Added (VEvent ev)])]))] |> ignore

printfn "tempPerRoom: %A" tempPerRoom.Value
printfn "hotRooms: %A" hotRooms.Value

let ev2 = event (DateTime.Now, Map.of_list [("room_id", VInt 2); ("temp", VInt 40)])
spread [(tempReadings, ([(0, [Added (VEvent ev2)])]))] |> ignore

printfn "tempPerRoom: %A" tempPerRoom.Value
printfn "hotRooms: %A" hotRooms.Value

let ev3 = event (DateTime.Now, Map.of_list [("room_id", VInt 2); ("temp", VInt 10)])
spread [(tempReadings, ([(0, [Added (VEvent ev3)])]))] |> ignore

printfn "tempPerRoom: %A" tempPerRoom.Value
printfn "hotRooms: %A" hotRooms.Value

Console.ReadLine() |> ignore