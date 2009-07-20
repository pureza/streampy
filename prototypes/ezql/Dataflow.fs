﻿#light

open System
open System.Text.RegularExpressions
open Extensions
open Ast
open Util
open Types
open TypeChecker
open Graph
open Oper
open CommonOpers
open AggregateOpers
open DictOpers
open Eval   

[<ReferenceEquality>]
type NodeInfo =
  { Uid:uid
    Type:Type
    mutable Name:string
    // uid * priority * parents * context -> created operator
    MakeOper:uid * Priority.priority * Operator list * Map<string, Operator> ref -> Operator
    ParentUids:uid list }
    
    member self.IsUnknown () = self.Type.IsUnknown ()

    interface IComparable with
      member self.CompareTo(other) =
        match other with
        | :? NodeInfo as other' -> Operators.compare self.Uid other'.Uid
        | _ -> failwith "Go away. Other is not even a NodeInfo."

    (* Creates a node with type "Unknown", no parents and an evaluation
       function that will never be called.
       Unknown nodes are ignored by the dataflow algorithm *)
    static member AsUnknown(uid, typ) =
      let makeOper' = fun (_, _, _, _) -> failwith "Won't be called!"

      { Uid = uid; Type = TyUnknown typ; Name = uid; MakeOper = makeOper';
        ParentUids = [] }

    static member UidOf(nodeInfo) = nodeInfo.Uid

and DataflowGraph = Graph<string, NodeInfo>
and NodeContext = Map<string, NodeInfo>

let checkInvariants (env:NodeContext) (types:TypeContext) (graph:DataflowGraph) =
  for pair in env do
    let key = pair.Key
    let value = pair.Value
    
    if not (Map.contains key types) then printfn "types doesn't contain a key that env contains: %s" key
    if types.[key] <> value.Type then printfn "Different types for %s! %A <> %A" key value.Type types.[key]
    if not (Graph.contains value.Uid graph)
      then if not (value.IsUnknown()) then printfn "env has a node that the graph doesn't contain: %s" value.Uid
      else if Graph.labelOf value.Uid graph <> value
             then printfn "env and graph have different nodes for %s" value.Uid


module ForwardDeps =
  (*
   * ForwardDeps attempts to solve the problem caused by an entity being
   * referenced before it is defined. Currently this happens with the
   * belongsTo/hasMany combo, and this implementation will probably
   * need to be modified to handle other cases.
   *
   * ForwardDeps is a kind of continuation. Basically, it allows the
   * dataflow algorithm to register requests such as "Hey, I was trying to
   * dataflow a Room.all and apparently I need to know what a Product.all is,
   * but I don't. So, I'm going to pause for now, but when you find out what a
   * Product.all is, please execute this action for me, to finish my job."
   *)

  type action = NodeContext -> TypeContext -> DataflowGraph -> DataflowGraph

  // When the type is known, call the following actions
  type ForwardDepsRep = Map<uid, action list>

  type ForwardDepsRec = { Add : uid * action -> unit; Contains : uid -> bool
                          Resolve : uid * NodeContext * TypeContext * DataflowGraph -> DataflowGraph
                          Reset : unit -> unit }

  let add uid action map : ForwardDepsRep =
    Map.add uid (if Map.contains uid map
                   then action::map.[uid]
                   else [action])
                map

  let mem = Map.contains
  let empty = Map.empty
  let is_empty = Map.isEmpty
  let resolve name (map:ForwardDepsRep) env types graph =
    List.fold (fun g a -> a env types g) graph map.[name]

let initTheForwardDeps () : ForwardDeps.ForwardDepsRec =
  let deps = ref ForwardDeps.empty
  { Add = fun (uid, action) -> deps := ForwardDeps.add uid action !deps
    Contains = fun uid -> ForwardDeps.mem uid !deps
    Resolve = fun (uid, env, types, graph) -> ForwardDeps.resolve uid !deps env types graph
    Reset = fun () -> deps := ForwardDeps.empty }    

let theForwardDeps : ForwardDeps.ForwardDepsRec = initTheForwardDeps ()

let nextSymbol =
  let counter = ref 0
  (fun prefix -> counter := !counter + 1
                 prefix + (!counter).ToString())

let uid2name uid = Regex.Match(uid, "(.*?)\d").Groups.[1].Value

(*
 * Creates a new node and adds it to the graph.
 * Returns both the node and the modified graph.
 *)
let createNode uid typ parents makeOp graph =
  let parentUids = List.map NodeInfo.UidOf parents
  let node = { Uid = uid; Type = typ; Name = uid2name uid
               MakeOper = makeOp; ParentUids = parentUids }
  node, Graph.add (parentUids, uid, node, []) graph

exception IncompleteLet of (string * NodeContext * TypeContext * DataflowGraph * expr) * string

let rec dataflowAnalysis types ast =
  let ticks, g = createNode "ticks" (TyStream (TyRecord (Map.of_list ["timestamp", TyInt]))) [] makeTicks Graph.empty
  let env = Map.empty.Add("ticks", ticks)
  let roots = ["ticks"]
  let env', types', g', roots' = List.fold dataflow (env, types, g, roots) ast

  //Graph.Viewer.display graph (fun v info -> (sprintf "%s (%s)" info.Name v))
  let operators = makeOperNetwork g' (Graph.nodes g') id Map.empty
  Map.fold_left (fun acc k v -> Map.add k operators.[v.Uid] acc) Map.empty env'


and dataflow (env:NodeContext, types:TypeContext, (graph:DataflowGraph), roots) = function
  | DefVariant _ -> env, types, graph, roots
  | Def (Identifier name, exp, None) ->
      let deps, g1, expr' = dataflowE env types graph exp
      let roots' = name::roots
      let n, g2 = makeFinalNode env types g1 expr' deps name
      n.Name <- name

      let env' = env.Add(name, n).Add(n.Uid, n)
      let types' = types.Add(n.Uid, n.Type)
      
      // Now that I know what a <name> is, check if there are any pending
      // expressions that depend on <name> to be fully dataflowed.
      let g3 = if theForwardDeps.Contains(name)
                 then theForwardDeps.Resolve (name, env', types', g2)
                 else g2
        
      env', types', g3, roots'
  | Expr expr ->
      let deps, g', expr' = dataflowE env types graph expr
      env, types, g', roots
  | StreamDef (Identifier name, fields) as def -> 
      let n, g' = createNode (nextSymbol "stream") types.[name] [] makeStream graph
      let roots' = name::roots
      n.Name <- name

      let env' = env.Add(name, n).Add(n.Uid, n)
      let types' = types.Add(n.Uid, n.Type)
      env', types', g', roots'
  | _ -> failwithf "By now, rewrite should have eliminated all the other possibilities."


and dataflowE (env:NodeContext) types (graph:DataflowGraph) expr =
  checkInvariants env types graph

  match expr with
  | MethodCall (target, (Identifier name), paramExps) ->
      let deps, g1, target' = dataflowE env types graph target
      if not ((typeOf types target).IsUnknown())
        then let n, g2 = makeFinalNode env types g1 target' deps ("<X>." + name + "()")
             let deps', g3, expr' = dataflowMethod env types g2 n name paramExps expr
             deps', g3, expr'
        else deps, g1, MethodCall (target', (Identifier name), paramExps)
  | ArrayIndex (target, index) ->
      match index with
      | Time (_, _) -> dataflowE env types graph (MethodCall (target, Identifier "[]", [index]))
      | _ -> let depsTrg, g1, target' = dataflowE env types graph target
             let depsIdx, g2, index' = dataflowE env types g1 index
             let deps = Set.union depsTrg depsIdx
             match typeOf types target with
             | TyDict typ when isContinuous types index' ->
                 let t, g3 = makeFinalNode env types g2 target' depsTrg "target[xxx]"
                 let i, g4 = makeFinalNode env types g3 index' depsIdx "xxx[i]"
                 let n, g5 = createNode (nextSymbol "[]") typ [t; i]
                                        (makeIndexer (Id (Identifier i.Uid))) g4
                 Set.singleton n, g5, Id (Identifier n.Uid)
             | _ -> deps, g2, ArrayIndex (target', index')
  | FuncCall (fn, paramExps) -> dataflowFuncCall env types graph fn paramExps expr
  | MemberAccess (target, (Identifier name)) ->
      let deps, g1, target' = dataflowE env types graph target
      let expr' = MemberAccess (target', (Identifier name))
      match typeOf types target with
      | TyRef (TyAlias entityType) ->
          // If it's a reference, create a special projector
          let entityDict = env.[entityDict entityType]
          let ref, g2 = makeFinalNode env types g1 target' deps "target->xxx"
          let projector, g3 = createNode (nextSymbol ".") (typeOf types expr) [ref; entityDict]
                                         (makeRefProjector name) g2
          Set.singleton projector, g3, Id (Identifier projector.Uid)      
      | TyType (_, fields, _, _) | TyRecord fields when isContinuous types target' ->
          let t, g2 = makeFinalNode env types g1 target' deps "target.xxx"
          let n, g3 = createNode (nextSymbol ("." + name)) fields.[name] [t]
                                 (makeProjector name) g2
          Set.singleton n, g3, Id (Identifier n.Uid)
      | _ -> deps, g1, MemberAccess (target', (Identifier name))
  | FixedAccess _ -> Set.empty, graph, expr
  | Record fields ->
      let deps, g', exprs =
        List.fold (fun (depsAcc, g, exprsAcc) (field, expr) ->
                       let deps, g', expr' = dataflowE env types g expr
                       Set.union depsAcc deps, g', exprsAcc @ [field, expr'])
                  (Set.empty, graph, []) fields
      
      deps, g', Record exprs  
  | Lambda (args, body) -> dataflowClosure env types graph expr None
  | Let (Identifier name, Some typ, (Lambda (args, binderBody) as binder), body) ->
      // Check if the binder is recursive
      let isRec = isRecursive name binder
      let (depsBinder:Set<NodeInfo>), g1, binder' = dataflowClosure env types graph binder (if isRec then Some (name, typ) else None)
      
      let closureNode, g2, depsBinder' =
        if isRec
          then NodeInfo.AsUnknown(name, typ), g1, depsBinder
          else let n, g2 = makeFinalNode env types g1 binder' depsBinder name
               n, g2, Set.singleton n

      let env', types' = env.Add(name, closureNode), types.Add(name, closureNode.Type)
      let depsBody, g3, body' = dataflowE env' types' g1 body
      Set.union depsBinder' depsBody, g3, Let (Identifier name, Some typ, binder', body')
  | Let (Identifier name, optType, binder, body) ->
      let depsBinder, g1, binder' =
        try
          dataflowE env types graph binder
        with
          | UnknownId id -> raise (IncompleteLet ((name, env, types, graph, body), id))
        
      // We must create a final node for the binder in order to extend the
      // environment with it, to dataflow the body.
      let binderNode, g2 = makeFinalNode env types g1 binder' depsBinder name
      let env' = env.Add(name, binderNode).Add(binderNode.Uid, binderNode)
      let types' = types.Add(name, binderNode.Type).Add(binderNode.Uid, binderNode.Type)
      let depsBody, g3, body' = dataflowE env' types' g2 body
      Set.add binderNode depsBody, g3, Let (Identifier name, optType, Id (Identifier binderNode.Uid), body')
  | If (cond, thn, els) ->
      let deps1, g1, cond' = dataflowE env types graph cond
      let deps2, g2, thn' = dataflowE env types g1 thn
      let deps3, g3, els' = dataflowE env types g2 els
      Set.union (Set.union deps1 deps2) deps3, g3, If (cond', thn', els')
  | Match (expr, cases) ->
      let depsExpr, g1, expr' = dataflowE env types graph expr
      let depsCases, g2, cases' =
        List.fold (fun (deps, graph, cases) (MatchCase (Identifier label, meta, body)) ->
                     let env', types' = match meta with
                                        | Some (Identifier name) ->
                                            let node = NodeInfo.AsUnknown(name, TyUnknown (metaTypeForLabel types label))
                                            env.Add(name, node), types.Add(name, node.Type)
                                        | _ -> env, types
                     let depsCase, g1, body' = dataflowE env' types' graph body
                     Set.union depsCase deps, g1, cases @ [MatchCase (Identifier label, meta, body')])
                  (Set.empty, g1, []) cases
      Set.union depsExpr depsCases, g2, Match (expr', cases')
  | BinaryExpr (oper, expr1, expr2) as expr ->
      let deps1, g1, expr1' = dataflowE env types graph expr1
      let deps2, g2, expr2' = dataflowE env types g1 expr2
      Set.union deps1 deps2, g2, BinaryExpr (oper, expr1', expr2')
  | Seq (expr1, expr2) ->
      let deps1, g1, expr1' = dataflowE env types graph expr1
      let deps2, g2, expr2' = dataflowE env types g1 expr2
      Set.union deps1 deps2, g2, Seq (expr1', expr2')
  | Id (Identifier name) ->
      let info = Map.tryFind name env
      match info with
      | Some info' -> if info'.Type.IsUnknown()
                        then Set.empty, graph, expr // If the type is unknown, nothing depends on it.
                        else Set.singleton info', graph, Id (Identifier info'.Uid)
                        
      | _ -> raise (UnknownId name)
  | Integer _ | String _ | Bool _ | Null -> Set.empty, graph, expr
  | _ -> failwithf "Expression type not supported: %A" expr

and dataflowMethod env types graph (target:NodeInfo) methName paramExps expr =
  match methName with
  | "sum" -> dataflowAggregate env types graph target paramExps makeSum methName expr
  | "count" -> dataflowAggregate env types graph target paramExps makeCount methName expr
  | "max" -> dataflowAggregate env types graph target paramExps makeMax methName expr
  | "min" -> dataflowAggregate env types graph target paramExps makeMin methName expr
  | "avg" -> dataflowAggregate env types graph target paramExps makeAvg methName expr
  | "any?" -> dataflowAnyAll env types graph target paramExps methName expr
  | "all?" -> dataflowAnyAll env types graph target paramExps methName expr
  | "last" -> dataflowAggregate env types graph target paramExps makeLast methName expr
  | "prev" -> dataflowAggregate env types graph target paramExps makePrev methName expr
  | "changes" -> let n, g' = createNode (nextSymbol "toStream") (TyStream (TyRecord (Map.of_list ["value", target.Type])))
                                        [target] (makeToStream) graph
                 Set.singleton n, g', Id (Identifier n.Uid)
  | "updates" -> let ancestors = getAncestorNodes target.Uid graph
                 let n, g' = createNode (nextSymbol "toStream") (TyStream (TyRecord (Map.of_list ["value", target.Type])))
                                        (target::ancestors) (makeToStream) graph
                 Set.singleton n, g', Id (Identifier n.Uid)                 
  | _ -> match target.Type with
          | TyStream fields ->
              match methName with
              | "[]" -> let duration = match paramExps with
                                       | [Time (Integer v, unit) as t] -> toSeconds v unit
                                       | _ -> failwith "Invalid duration"
                        let n, g' = createNode (nextSymbol "[x min]") (TyWindow (target.Type, TimedWindow duration)) [target]
                                               (makeWindow duration) graph
                        Set.singleton n, g', Id (Identifier n.Uid)
              | "where" -> dataflowWhere env types graph target paramExps expr
              | "select" -> dataflowSelect env types graph target paramExps expr
              | "groupby" -> dataflowGroupby env types graph target paramExps expr
              | _ -> failwithf "Unkown method: %s" methName
          | TyWindow _ ->
              match methName with
              | "where" -> dataflowWhere env types graph target paramExps expr
              | "select" -> dataflowSelect env types graph target paramExps expr
              | "groupby" -> dataflowGroupby env types graph target paramExps expr
              | _ -> failwithf "Unkown method of type Window: %s" methName
          | TyDict valueType ->
              match methName with
              | "where" -> dataflowDictOps env types graph target paramExps valueType makeInitialOp valueType makeDictWhere (nextSymbol methName) expr
              | "select" ->
                  let arg, body =
                    match paramExps with
                    | [Lambda ([Param (Identifier arg, _)], body) as fn] ->
                        arg, body
                    | _ -> failwith "Invalid parameter to dict/select"
                  let projType = typeOf (types.Add(arg, valueType)) body
                  dataflowDictOps env types graph target paramExps valueType makeInitialOp projType makeDictSelect (nextSymbol methName) expr
              | "values" -> let n, g' = createNode (nextSymbol "values") (TyWindow (valueType, Unbounded))
                                                   [target] (makeValues) graph   
                            Set.singleton n, g', Id (Identifier n.Uid)       
              | _ -> failwithf "Unkown method: %s" methName
          | TyInt -> match methName with
                     | "[]" -> let duration = match paramExps with
                                              | [Time (Integer v, unit) as t] -> toSeconds v unit
                                              | _ -> failwith "Invalid duration"
                               let n, g' = createNode (nextSymbol "[x min]") (TyWindow (target.Type, TimedWindow duration))
                                                      [target] (makeDynValWindow duration) graph
                               Set.singleton n, g', Id (Identifier n.Uid)
                     | _ -> failwithf "Unkown method: %s" methName
          | TyBool -> match methName with
                      | "[]" -> let duration = match paramExps with
                                               | [Time (Integer v, unit) as t] -> toSeconds v unit
                                               | _ -> failwith "Invalid duration"
                                let n, g' = createNode (nextSymbol "[x min]") (TyWindow (target.Type, TimedWindow duration))
                                                       [target] (makeDynValWindow duration) graph
                                Set.singleton n, g', Id (Identifier n.Uid)
                      | "howLong" -> let n, g' = createNode (nextSymbol "howLong") TyInt
                                                            [target; env.["ticks"]] (makeHowLong) graph
                                     Set.singleton n, g', Id (Identifier n.Uid)
                      | _ -> failwithf "Unkown method: %s" methName       
          | TyVariant _ -> match methName with
                           | _ -> failwithf "Unkown method: %s" methName
          | TyRecord _ -> match methName with
                          | _ -> // This may happen if the record field contains a function. 
                                 // So, convert the method call into a function call and proceed.
                                 let expr' = FuncCall (MemberAccess (Id (Identifier target.Uid), Identifier methName), paramExps)
                                 dataflowE (env.Add(target.Uid, target)) (types.Add(target.Uid, target.Type)) graph expr'
          | _ -> failwith "Unknown target type"

and dataflowFuncCall (env:NodeContext) (types:TypeContext) graph func paramExps expr =
  // Used to dataflow primitive (print, ...) and recursive functions
  let dataflowByEval () =
    // Dataflow the function itself, but only if it's not a primitive one.
    let deps1, g1, fnExpr' =
      match func with
      | Id (Identifier "print") -> Set.empty, graph, func
      | Id (Identifier "$makeEnum") -> Set.empty, graph, func
      | _ -> dataflowE env types graph func
    let deps2, g2, paramExps' = List.fold (fun (depsAcc, g, exprs) expr ->
                                             let deps, g', expr' = dataflowE env types g expr
                                             Set.union depsAcc deps, g', exprs @ [expr'])
                                          (Set.empty, g1, List.empty) paramExps
    Set.union deps2 deps1, g2, FuncCall (fnExpr', paramExps')

  match func with
  | Id (Identifier "stream") ->
      let n, g' = createNode (nextSymbol "stream") (typeOf Map.empty expr) [] makeStream graph
      Set.singleton n, g', Id (Identifier n.Uid)
  | Id (Identifier "when") ->
      match paramExps with
      | [target; Lambda ([Param (Identifier arg, _)], body)] ->      
          // Dataflow the target
          let depsTarget, g1, target' = dataflowE env types graph target
          let targetNode, g2 = makeFinalNode env types g1 target' depsTarget "when-target"

          let evType = match targetNode.Type with
                       | TyStream t -> t
                       | _ -> failwithf "Can't happen"

          // Dataflow the body
          let argNode = NodeInfo.AsUnknown(arg, evType)
          let env', types' = env.Add(arg, argNode), types.Add(arg, argNode.Type)
          let depsBody, g3, body' = dataflowE env' types' g2 body

          let handler = Lambda ([Param (Identifier arg, Some evType)], body')
          let n, g4 = createNode (nextSymbol "when") (typeOf (types.Add(arg, evType)) body) (targetNode::(Set.to_list depsBody))
                                 (makeWhen handler) g3
          Set.singleton n, g4, Id (Identifier n.Uid)  
      | _ -> failwithf "Invalid parameters to when: %A" paramExps
  | Id (Identifier "$ref") ->
      match paramExps with
      | [innerExpr] ->
          let deps', g1, innerExpr' = dataflowE env types graph innerExpr
          
          // Get the type of the referenced object and create a function that
          // will return its unique id
          let entity, refType = match typeOf types innerExpr with
                                | TyType (name, _, id, _) -> name, id
                                | _ -> failwithf "Only entities may be referenced"
          let getId = fun entity -> match entity with
                                    | VRecord m -> m.[VString refType]
                                    | _ -> failwithf "The entity is not a record?!"
          let n, g2 = makeFinalNode env types g1 innerExpr' deps' "{ }"
          // The type must be a TyRef (TyAlias entity), so that dataflowE will create a refProjector.
          let ref, g3 = createNode (nextSymbol "ref") (TyRef (TyAlias entity)) [n]
                                   (makeRef getId) g2
          Set.singleton ref, g3, Id (Identifier ref.Uid)
      | _ -> invalid_arg "paramExps"
  | Id (Identifier "listenN") ->
      match paramExps with
      | initial::listeners ->
          let depsInitial, g1, initial' = dataflowE env types graph initial
          let initialNode, g2 = makeFinalNode env types g1 initial' depsInitial "listenN-initial"
          let parents, g3 =
            List.fold (fun (parents, graph) listener ->
                         let deps, g', _ = dataflowE env types graph listener
                         assert (Set.count deps = 1)
                         parents @ [deps.MinimumElement], g')
                      ([], g2) listeners
          let node, g4 = createNode (nextSymbol "listenN") initialNode.Type (initialNode::parents)
                                    makeListenN g3
          Set.singleton node, g4, Id (Identifier node.Uid)
      | [] -> failwithf "No listeners to listenN"
  | Id (Identifier "now") -> Set.empty, graph, expr
  | Id (Identifier "print") -> dataflowByEval ()
  | Id (Identifier "$makeEnum") -> dataflowByEval ()
  | Id (Identifier "merge") ->
      match paramExps with
      | [stream1; stream2; SymbolExpr (Symbol field)] ->
          let deps1, g1, stream1' = dataflowE env types graph stream1
          let stream1Node, g2 = makeFinalNode env types g1 stream1' deps1 "merge-stream1"
          
          let deps2, g3, stream2' = dataflowE env types g2 stream2
          let stream2Node, g4 = makeFinalNode env types g3 stream2' deps2 "merge-stream2"
          
          let allFields = match typeOf types expr with
                          | TyStream (TyRecord fields) -> Map.fold_left (fun acc k v -> Set.add k acc) Set.empty fields
                          | _ -> failwithf "merge results in a stream"
          let node, g5 = createNode (nextSymbol "merge") (typeOf types expr) [stream1Node; stream2Node] 
                                    (makeMerge (VString field) allFields) g4
          Set.singleton node, g5, Id (Identifier node.Uid)
      | _ -> failwithf "Invalid parameters to merge."
  | _ when not (isContinuous types expr) -> dataflowByEval ()
  | _ -> // Dataflow the function expression itself
         let funDeps, g1, func' = dataflowE env types graph func
         let funcNode, g2 = makeFinalNode env types g1 func' funDeps (func'.Name)
         
         // Dataflow parameters
         let paramDeps, g3, paramExps' =
           List.fold (fun (paramNodes, g, exprs) expr ->
                        let deps, g1, expr' = dataflowE env types g expr
                        // Each parameter gets its own node
                        let paramNode, g2 = makeFinalNode env types g1 expr' deps expr'.Name
                        paramNodes @ [paramNode], g2, exprs @ [Id (Identifier paramNode.Uid)])
                     ([], g2, List.empty) paramExps
      
         let returnType = typeOf types expr
         let n, g4 = createNode (nextSymbol expr.Name) returnType (funcNode::paramDeps)
                                (makeFuncCall) g3
         Set.singleton n, g4, Id (Identifier n.Uid)


and dataflowGroupby env types graph target paramExps =
  let argType = target.Type
  let argMaker = match argType with
                 | TyStream _ -> makeStream
                 | TyWindow (TyStream _, TimedWindow _) -> makeSimpleWindow
                 | _ -> failwithf "GroupBy's argument must be a stream or an event window."
  let field, body, arg =
    match paramExps with
    | [SymbolExpr (Symbol field); Lambda ([Param (Identifier arg, _)], body) as fn] ->
        let argInfo = { Uid = arg; Type = argType; MakeOper = argMaker; Name = arg; ParentUids = [] }
        field, body, arg
    | _ -> failwith "Invalid parameter to groupby"
  let valueType = typeOf (types.Add(arg, argType)) body
    
  dataflowDictOps env types graph target [Lambda ([Param (Identifier arg, None)], body)]
                  argType argMaker valueType (makeGroupby field) (nextSymbol "groupBy")

(*
 * Dataflows stream.groupby(), dict.select() and dict.where().
 *)
and dataflowDictOps (env:NodeContext) (types:TypeContext) graph target paramExps argType argMaker resType opMaker nodeUid expr =
  let rec keepTrying arg (env:NodeContext) (types:TypeContext) graph body =
    let argInfo = env.[arg]
    let argType = types.[arg]
    
    // What to do when we find the "true nature" of the forward dependency
    let action theDep env types (graph:DataflowGraph) =
      let deps, (g':DataflowGraph), opResult, subExprBuilder =
        match dataflowSubExpr env types graph with
        | deps, g', _, Some (opResult, subExprBuilder) -> deps, g', opResult, subExprBuilder
        | _ -> failwithf "dataflowSubExpr returned unexpected results."

      // Modify the node and add it to the graph
      let (p, uid, info, s) = g'.[nodeUid]
      let n = { info with MakeOper = opMaker subExprBuilder }
      
      let theDepUid = env.[theDep].Uid
      
      // Correct the order of dependencies between belongsTo/hasMany associations.
      // The entity on the hasMany side should be an ancestor of the belongsTo.
      match n.Type with
      | TyDict (TyType (_, _, _, [])) ->
          // n is on the hasMany side
          Graph.add (p, n.Uid, n, s) g'
      | TyDict (TyType (_, _, _, x::xs)) ->  
          // n is on the belongsTo side
          let g' = Graph.add (theDepUid::p, n.Uid, { n with ParentUids = n.ParentUids @ [theDepUid] }, s) graph
         
          // Now remove n from the hasMany ancestors, if it's there.
          let (p, uid, info, s), g2 =
            match Graph.extract theDepUid g' with
            | Some (ctx), g'' -> ctx, g''
            | _ -> failwithf "Can't happen because %A is supposed to be on the graph" theDepUid
           
          Graph.add (List.remove n.Uid p, uid, { info with ParentUids = List.remove n.Uid info.ParentUids }, s) g2
      | _ -> failwithf "Won't happen because all forward dependencies are related to entities"
                       
      
    // Remove a field from an entity dictionary declaration
    // Remember that these declarations have the following form:
    // let $a = ...
    // let $b = ...
    // { a = $a, b = $b }
    //
    // If a is removed and b depends on a, then b is also removed.
    let rec removeField field expr =
      match expr with
      | Let (Identifier field', optType, binder, body) ->
          if Set.contains field (freeVars binder)
            then removeField field' (removeField field body)
            else Let (Identifier field', optType, binder, removeField field body)
      | Record fields -> Record (List.filter (fun (_, expr) -> (not (Set.contains field (freeVars expr)))) fields)
      | _ -> failwithf "removeField: Unexpected expression %A" expr

    // Try the normal dataflow first. If there is an error, we add "action" as a continuation.
    retry (fun () -> dataflowE env types graph body)
          (fun err -> match err with
                      | IncompleteLet ((field, env, types, graph, letBody), causeId) ->
                          theForwardDeps.Add(causeId, action causeId)
                          
                          // Remove the missing field from the declaration and proceed
                          let letBody' = removeField field letBody
                          dataflowE env types graph letBody'
                      | _ -> raise err)

  and dataflowSubExpr (env:NodeContext) (types:TypeContext) (graph:DataflowGraph) =
    let body, env', types', g1, arg =
      match paramExps with
      | [Lambda ([Param (Identifier arg, _)], body) as fn] ->
          let argUid = nextSymbol arg
          let argInfo = { Uid = argUid; Type = argType; MakeOper = argMaker; Name = arg; ParentUids = [] }
          let env' = env.Add(arg, argInfo).Add(argUid, argInfo)
          let types' = types.Add(arg, argType).Add(argUid, argType)
          body, env', types', graph.Add([], argUid, argInfo, []), argUid
      | _ -> invalid_arg "paramExps"
      
    extractSubNetwork env' types' g1 (keepTrying arg) [arg] body
     
  let deps, g1, opResult, subExprBuilder =
    match dataflowSubExpr env types graph with
    | deps, g1, _, Some (opResult, subExprBuilder) -> deps, g1, opResult, subExprBuilder
    | _ -> failwithf "dataflowSubExpr returned unexpected results."

  // FIXME: Ensure the following is still applicable.
  // If the body itself does not depend on the argument (e.g., t -> some-expression-without-t),
  // then it is a "constant" and we can simply mark it as a dependency of the operation.
  // To ensure it doesn't depend on the argument, check if it is present in g1.
  let opDeps = target::(Set.to_list (if Graph.contains opResult.Uid g1 then Set.add opResult deps else deps))

  let n, g2 = createNode nodeUid (TyDict resType) opDeps
                         (opMaker subExprBuilder) g1
  Set.singleton n, g2, Id (Identifier n.Uid)

and makeSubExprBuilder roots final graph : NetworkBuilder =
  // prio is the priority of the parent of the subnetwork
  fun prio context ->
    let fixPrio p = Priority.add (Priority.down prio) p
    let context' = makeOperNetwork graph roots fixPrio context
    (List.map (fun root -> context'.[root]) roots), context'.[final]

(*
 * Dataflows a lambda whose network will be needed later.
 * This applies to:
 *   - stream.groupby(field, fun g -> ...) (the subexpression's network will be needed for each group)
 *   - dict.where(fun t -> ...) (idem)
 *   - dict.select(fun t -> ...) (idem)
 *   - let f = fun a b c -> ... in f(x, y, z) (the subexpression's network will be needed for each call)
 *
 * Returns:
 *   - The final operator of the subexpression
 *   - The global dependencies of the lambda's body
 *   - The resulting graph (without the subexpression's network)
 *   - A function that, when called, will recreate the subexpression's network on demand.
 *)
and extractSubNetwork env types graph dataflowBodyFn args body : Set<NodeInfo> * DataflowGraph * expr * (NodeInfo * NetworkBuilder) option =
  let deps, g2, body' = dataflowBodyFn env types graph body

  if isContinuous types body
    then // Make a final node for the evaluation of the body, if necessary
         let opResult, g2' = makeFinalNode env types g2 body' deps "{ }"

         // Remove from g2 everything that depends on the argument
         // and update the set of dependencies
         let deps', g3 = removeNetworks args deps g2'
         
         // Uses g2 because it contains the group's subnetwork
         deps', g3, body', Some (opResult, makeSubExprBuilder args opResult.Uid g2')
    else let deps', g3 = removeNetworks args deps g2
         deps', g3, body', None


and dataflowClosure env types graph expr (itself:(string * Type) option) =
  (* Recursive closures are evaluated normally: no network is created for them.
   * They need, however, to be dataflown, to gather global dependencies. *)
  let recClosure args body name typ =
    let env', types'  =
      List.fold (fun (env:NodeContext, types:TypeContext) (Param (Identifier arg, typ)) ->
                   match typ with
                   | Some t -> let node = NodeInfo.AsUnknown(arg, resolveAlias types t)
                               env.Add(arg, node), types.Add(arg, node.Type)
                   | None -> failwithf "Function arguments must have type annotations.")
                (env, types) args
                
    // Add itself as an unknown node to dataflow the body.              
    // (Unknown nodes in function calls are ignored, but their parameters are dataflown normally)
    let myNode = NodeInfo.AsUnknown(name, typ)
    let env'', types'' = env'.Add(name, myNode), types'.Add(name, myNode.Type)
    let deps, g', body' = dataflowE env'' types'' graph body
    
    deps, g', Lambda (args, body'), None
    

  let nonRecClosure args body =
    // Extract the body's network, so that we can recreate it everytime we
    // call the closure.
    let env', types', g1, args', argUids =
      List.fold (fun (env:NodeContext, types:TypeContext, graph, args, argUids) (Param (Identifier arg, typ)) ->
                   match typ with
                   | Some t -> let argUid = nextSymbol arg
                               let t' = resolveAlias types t
                               let node = { Uid = argUid; Type = t'; MakeOper = makeInitialOp; Name = arg; ParentUids = [] }
                               let env' = env.Add(arg, node).Add(argUid, node)
                               let types' = types.Add(arg, node.Type).Add(argUid, node.Type)
                               let args' = args @ [Param ((Identifier argUid), Some t')]
                               env', types', Graph.add ([], argUid, node, []) graph, args', argUids @ [argUid]
                   | None -> failwithf "Function arguments must have type annotations.")
                (env, types, graph, [], []) args

    let deps, g2, body', network = extractSubNetwork env' types' g1 dataflowE argUids body
    deps, g2, Lambda (args', body'), network

  let args, body =
    match expr with
    | Lambda (args, body) -> args, body
    | _ -> invalid_arg "expr"
             
  // FIXME: Compare with dataflowdictOps. Do I need to check if opResult is in g2 and add it as a dep?
  let deps, g', newLambda, network =
    match itself with
    | Some (name, typ) -> recClosure args body name typ
    | None -> nonRecClosure args body
 
  match network with
  | Some (_,  networkBuilder) ->    
      let ids = (List.fold (fun acc (Param (Identifier id, _)) -> acc + id + ".") "" args)
      let uid = sprintf "λ%s" ids
      let n, g'' = createNode (nextSymbol uid) (typeOf types expr) (Set.to_list deps)
                              (makeClosure newLambda networkBuilder (Option.bind (fst >> Some) itself)) g'      
      Set.singleton n, g'', Id (Identifier n.Uid)
  | None -> deps, g', newLambda      


and dataflowAggregate env types graph target paramExprs opMaker aggrName expr =
  let field = match paramExprs with
              | [SymbolExpr (Symbol name)] -> name
              | _ -> ""
  let getMaker name = function
    | VRecord fields -> fields.[VString name]
    | other -> failwithf "getMaker expects events but was called with a %A" other
                   
  let getField = match paramExprs, target.Type with
                 | [], _ when aggrName = "count" -> id
                 | [SymbolExpr (Symbol name)], TyStream _ -> getMaker name
                 | [SymbolExpr (Symbol name)], TyWindow _ -> getMaker name
                 | [], TyInt -> id
                 | [], TyBool -> id
                 | [], TyWindow (TyInt, _) -> id
                 | _ -> failwithf "Invalid parameters to %s" aggrName
  let n, g' = createNode (nextSymbol aggrName) (typeOf types expr) [target]
                         (opMaker getField) graph
  n.Name <- (sprintf "%s(%s)" aggrName field)
  Set.singleton n, g', Id (Identifier n.Uid)

and dataflowAnyAll env types graph target paramExprs methName expr =
  let valueType = match target.Type with
                  | TyWindow (TyStream v, _) | TyWindow (v, _) | TyStream v | v -> v
                 
  let subExpr, env', types', arg =
    match paramExprs with
    | [Lambda ([Param (Identifier arg, _)], body) as fn] ->
        // Put the argument as an Unknown node into the environment
        // This way it will be ignored by the dataflow algorithm
        let argNode = NodeInfo.AsUnknown(arg, valueType)
        body, env.Add(arg, argNode), types.Add(arg, argNode.Type), arg
    | [] -> // For booleans
        let arg = "$v"
        let argNode = NodeInfo.AsUnknown(arg, valueType)
        BinaryExpr (op.Equal, Id (Identifier arg), Bool true), env.Add(arg, argNode), types.Add(arg, argNode.Type), arg
    | _ -> failwith "Invalid parameter to any?/all?"
    
  let deps, g', expr' = dataflowE env' types' graph subExpr
  let expr'' = Lambda ([Param (Identifier arg, None)], expr')
                  
  // Depends on the stream and on the dependencies of the predicate.
  let opDeps = target::(Set.to_list deps)
  let n, g'' = createNode (nextSymbol methName) TyBool opDeps
                          (makeAnyAll (methName = "any?") expr'') g'
  Set.singleton n, g'', Id (Identifier n.Uid)


(* Returns the ancestor roots of any given node in the graph *)
and getAncestorRoots node graph =
  match graph with
  | Extract node ((p, _, v, _), gr) ->
      if p.IsEmpty
        then Set.singleton v
        else List.fold (fun acc a -> Set.union acc (getAncestorRoots a gr)) Set.empty p
  | _ -> failwithf "getAncestorRoots: Can't find the node in the graph"

(* Returns the ancestors of any given node in the graph *)
and getAncestorNodes node graph =
  match graph with
  | Extract node ((p, _, _, _), gr) -> List.map (fun x -> Graph.labelOf x graph) p
  | _ -> failwithf "getAncestors: Can't find the node in the graph"
  

(*
and renameNetwork renamer root graph =
  let rec rebuildGraph (trees:Graph.Algorithms.Tree<Graph.Context<string, NodeInfo>> list) graph =
    List.fold (fun acc x -> addTree x acc) graph trees
  and addTree tree graph =
    let g' = rebuildGraph tree.forest graph
    let (pred, n, info, s) = tree.root
    
    // Keep only the parents that kept their names
    let pred' = List.filter (fun parent -> Graph.mem parent g') pred
    let info' = { info with Uid = n; ParentUids = List.map (fun p -> if Graph.mem p g' then p else renamer p) info.ParentUids }
    Graph.add (pred', n, info', s) g'
    
  let renamer' (p, n, info, s) = (p, renamer n, info, List.map renamer s)  
  let tree, g' = Graph.Algorithms.dfsWith Graph.suc' renamer' [root] graph
  rebuildGraph tree g'
*)  

(*
 * Handles <stream|window>.select()
 *)
and dataflowSelect env types graph target paramExps expr =
  let inEvType, isWindow =
    match target.Type with
    | TyStream evType -> evType, false
    | TyWindow (TyStream evType, _) -> evType, true
    | _ -> failwithf "stream.select can only be applied to streams and windows"
                 
  let subExpr, env', types', arg =
    match paramExps with
    | [Lambda ([Param (Identifier arg, _)], body) as fn] ->
        // Put the argument as an Unknown node into the environment
        // This way it will be ignored by the dataflow algorithm
        let argNode = NodeInfo.AsUnknown(arg, inEvType)
        body, env.Add(arg, argNode), types.Add(arg, argNode.Type), arg
    | _ -> failwith "Invalid parameter to where"
  let deps, g', expr' = dataflowE env' types' graph subExpr
  let expr'' = Lambda ([Param (Identifier arg, None)], expr')
 
  // Depends on the parent and on the dependencies of the predicate.
  let opDeps = target::(Set.to_list deps)
  let n, g'' = createNode (nextSymbol "Select") (typeOf types expr) opDeps
                          (makeSelect expr'' isWindow) g'
  Set.singleton n, g'', Id (Identifier n.Uid)
  
  
(*
 * Handles stream.where()
 *)
and dataflowWhere env types graph target paramExps expr =
  let evType, isWindow =
    match target.Type with
    | TyStream evType -> evType, false
    | TyWindow (TyStream evType, _) -> evType, true
    | _ -> failwithf "stream.select can only be applied to streams and windows"

  let subExpr, env', types', arg =
    match paramExps with
    | [Lambda ([Param (Identifier arg, _)], body) as fn] ->
        // Put the argument as an Unknown node into the environment
        // This way it will be ignored by the dataflow algorithm
        let argNode = NodeInfo.AsUnknown(arg, evType)
        body, env.Add(arg, argNode), types.Add(arg, argNode.Type), arg
    | _ -> failwith "Invalid parameter to where"
  let deps, g', expr' = dataflowE env' types' graph subExpr
  let expr'' = Lambda ([Param (Identifier arg, None)], expr')
 
  // Depends on the stream and on the dependencies of the predicate.
  let opDeps = target::(Set.to_list deps)
  let n, g'' = createNode (nextSymbol "Where") target.Type opDeps
                          (makeWhere expr'' isWindow) g'
  Set.singleton n, g'', Id (Identifier n.Uid)

and isEntityInit typ =
  match typ with
  | TyType _ -> true
  | _ -> false
  
and extractRecord expr =  
  match expr with
  | Let (_, _, _, body) -> extractRecord body
  | _ -> expr

(* Create the necessary nodes to evaluate an expression.
 *  - If the expression is a simple variable access, no new nodes are necessary;
 *  - If the expression is a record, we will need new nodes for each field and
 *    another node for the entire record;
 *  - If the expression is of any other kind, we create a general purpose
 *    evaluator node.
 *)
and makeFinalNode env types graph expr deps name =
  (* Extend the environment to include dependencies of all the subexpressions *)
  let env' = Set.fold (fun acc info -> Map.add info.Uid info acc) env deps
  let types' = Set.fold (fun acc info -> Map.add info.Uid info.Type acc) types deps

  assert (isContinuous types' expr)
      
  match expr with
  (* No additional node necessary *)
  | Id (Identifier uid) -> deps.MinimumElement, graph
 
  (* Yes, the result is a record and we need the corresponding operator *)
  | Record fields ->
      let g'', fieldDeps =
        List.fold (fun (g, fieldDeps) (field, expr) ->
                     (* Lets get the dependencies for just this expression *)
                     let deps'', g', expr' = dataflowE env' types' g expr
                    
                     (* Sanity check: at this point, the expression should be completely
                        "continualized" and thus, the previous operation must not have
                        altered the graph nor the expression itself *)
                     assert (g' = g && expr' = expr)
                     
                     (* Do we need an evaluator just for this field? *)
                     let n, g' = makeFinalNode env' types' g expr deps'' field
                     n.Name <- field
                     g', (field, n)::fieldDeps)
                  (graph, []) fields

      let uids = List.map snd fieldDeps
      let fieldTypes = Map.of_list (List.map (fun (f, n) -> (f, n.Type)) fieldDeps)                                  
      createNode (nextSymbol name) (TyRecord fieldTypes) uids
                 // At runtime, we need to find the operators corresponding to the parents
                 (fun (uid, prio, parents, context) ->
                    let parentOps = List.map (fun (f, n) ->
                                                (f, List.find (fun (p:Operator) -> p.Uid = n.Uid) parents))
                                             fieldDeps
                    makeRecord parentOps (uid, prio, parents, context))
                 g''

  (* If this is an entity initialization (i.e. let a = ... in let b = ... in { a = $a, b = $b }),
     ignore all the lets and create a final node just for the record *)
  | _ when isEntityInit (typeOf types' expr) -> makeFinalNode env types graph (extractRecord expr) deps name
  
  (* The result is an arbitrary expression and we need to evaluate it *)
  | _ -> createNode (nextSymbol name) (typeOf types' expr) (Set.to_list deps)
                    (makeEvaluator expr Map.empty) graph


(* Remove a given node and its descendents from the set of dependencies, maintaing
   coherence. That is, dependencies of nodes that are removed should be added to
   the resulting set of dependencies, unless they are to be removed too. *)
and removeNetwork root deps graph =
  match graph with
  | Extract root ((p, _, info, s), g') ->
      let pi = List.map (fun uid -> Graph.labelOf uid graph) p
      // Add the parents to the dependencies and remove this node
      let deps' = Set.union (Set.of_list pi) (Set.remove info deps)
      
      // Remove each successor
      List.fold (fun (deps, g) s -> removeNetwork s deps g) (deps', g') s
  | _ -> deps, graph

and removeNetworks roots deps graph =
  List.fold (fun (deps, graph) root -> removeNetwork root deps graph) (deps, graph) roots
  
  
(*
 * Iterate the graph and create the operators and the connections between them.
 *)
and makeOperNetwork (graph:DataflowGraph) (roots:string list) fixPrio context : Map<string, Operator> =
  let rec spreadInitialChanges initialChanges =
    for (op:Operator, (changes:changes)) in initialChanges do
      op.AllChanges := [changes]
    let stack = List.fold (fun stack (op, changes) -> mergeStack stack (toEvalStack op.Children changes)) [] initialChanges
    retrySpread stack []
    
  // Spread all changes in the stack. If some step fails, try the remaining steps.
  and retrySpread stack delayed =
    try
      spread stack delayed
    with
      | SpreadException (_, rest, delayed, _) -> retrySpread rest delayed
  

  // GroupBy's should be visited first because of belongsTo/hasMany association.
  let next (_, _, _, s) = List.sortBy (fun (x:string) -> if x.StartsWith("groupBy") then 1 else 0) s 
  let order = Graph.Algorithms.topSort next Graph.node' roots graph

  //printfn "order: %A" order
  //printfn "operators: %A" (Map.to_list operators |> List.map fst)
  //Graph.Viewer.display graph (fun v info -> (sprintf "%s (%s)" info.Name v))
  //Graph.Viewer.display graph (fun v info -> info.Name)
  
  // Maps uids to priorities
  let orderPrio = List.fold (fun (acc, prio) x -> (Map.add x prio acc, Priority.next prio))
                            (Map.empty, Priority.initial) order |> fst

  // Keep a container with all the operators and provide a way so that each
  // operator is able to see it.
  let contextRef = ref Map.empty

  // Fold the graph with the right order, returning a map of all the operators
  let context', initialChanges =
    Graph.foldSeq (fun (context, changes) (pred, uid, info, succ) ->
                     // If the operator already exists, skip it.
                     if Map.contains uid context
                       then context, changes
                       else //printfn "Vou criar o %s que tem como pais %A %A" uid info.ParentUids pred
                            let parents = List.map (fun uid -> match Map.tryFind uid context with
                                                               | Some op -> op
                                                               | _ -> printfn "The operators map doesn't have operator %s" uid
                                                                      failwithf "shit")
                                                    info.ParentUids
                            let op = info.MakeOper (uid, fixPrio orderPrio.[uid], parents, contextRef)
                            let changes' = if op.Value <> VNull then (op, [Added (op.Value)])::changes else changes
                            //printfn "Created operator %s with priority %A" uid (fixPrio orderPrio.[uid])
                            Map.add uid op context, changes')
                  (context, []) graph order
                  
  // This magical line will set the Context field of all the operators created
  // in this iteration to point to the same map.                  
  contextRef := context'          
  spreadInitialChanges initialChanges
  context'               