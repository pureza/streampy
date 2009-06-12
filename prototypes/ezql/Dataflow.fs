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

    interface IComparable with
      member self.CompareTo(other) =
        match other with
        | :? NodeInfo as other' -> Operators.compare self.Uid other'.Uid
        | _ -> failwith "Go away. Other is not even a NodeInfo."

    (* Creates a node with type "Unknown", no parents and an evaluation
       function that will never be called.
       Unknown nodes are ignored by the dataflow algorithm *)
    static member AsUnknown(uid) =
      let makeOper' = fun (_, _, _, _) -> failwith "Won't happen!"

      { Uid = uid; Type = TyUnit; Name = uid; MakeOper = makeOper';
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
      then if value.Type <> TyUnit then printfn "env has a node that the graph doesn't contain: %s" value.Uid
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

exception IncompleteRecord of (NodeInfo Set * DataflowGraph * expr) * string Set

let rec dataflow (env:NodeContext, types:TypeContext, (graph:DataflowGraph), roots) = function
  | Def (Identifier name, exp) ->
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
  | _ -> failwithf "By now, rewrite should have eliminated all the other possibilities."


and dataflowE (env:NodeContext) types (graph:DataflowGraph) expr =
  checkInvariants env types graph
  match expr with
  | MethodCall (target, (Identifier name), paramExps) ->
      let deps, g1, target' = dataflowE env types graph target
      if (typeOf types target) <> TyUnit
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
             | TyDict (TyEntity entity) -> 
                 let t, g3 = makeFinalNode env types g2 target' depsTrg "target[xxx]"
                 let i, g4 = makeFinalNode env types g3 index' depsIdx "xxx[i]"
                 let n, g5 = createNode (nextSymbol "[]") (TyEntity entity) [t; i]
                                        (makeIndexer (Id (Identifier i.Uid))) g4
                 Set.singleton n, g5, Id (Identifier n.Uid)
             | _ -> deps, g2, ArrayIndex (target', index')
  | FuncCall (fn, paramExps) -> dataflowFuncCall env types graph fn paramExps expr
  | MemberAccess (target, (Identifier name)) ->
      let deps, g1, target' = dataflowE env types graph target
      let expr' = MemberAccess (target', (Identifier name))
      match typeOf types target with
      | TyRef (TyEntity entityType) ->
          // If it's a reference, create a special projector
          let entityDict = env.[entityDict entityType]
          let ref, g2 = makeFinalNode env types g1 target' deps "target->xxx"
          let projector, g3 = createNode (nextSymbol ".") (typeOf types expr) [ref; entityDict]
                                         (makeRefProjector name) g2
          Set.singleton projector, g3, Id (Identifier projector.Uid)
      | TyEntity entity ->
          let fields = match types.[entity] with
                       | TyType (fields, _) -> fields
                       | _ -> failwithf "entity is not a TyType?!"
          let t, g2 = makeFinalNode env types g1 target' deps "target.xxx"
          let n, g3 = createNode (nextSymbol ("." + name)) fields.[name] [t]
                                 (makeProjector name) g2
          Set.singleton n, g3, Id (Identifier n.Uid) 
      | TyRecord fields ->
          let t, g2 = makeFinalNode env types g1 target' deps "target.xxx"
          let n, g3 = createNode (nextSymbol ("." + name)) fields.[name] [t]
                                 (makeProjector name) g2
          Set.singleton n, g3, Id (Identifier n.Uid) 
      | _ -> deps, g1, MemberAccess (target', (Identifier name))
  | Record fields ->
      let deps, g', exprs, unknown =
        List.fold (fun (depsAcc, g, exprsAcc, unknown) (field, expr) ->
                     try
                       let deps, g', expr' = dataflowE env types g expr
                       Set.union depsAcc deps, g', exprsAcc @ [field, expr'], unknown
                     with
                       | UnknownId id -> depsAcc, g, exprsAcc, Set.add id unknown)
                  (Set.empty, graph, [], Set.empty) fields
                  
      // If some field could not be dataflowed, raise an exception.
      if (Set.isEmpty unknown)
        then deps, g', Record exprs
        else raise (IncompleteRecord ((deps, g', Record exprs), unknown))
  | Lambda (args, body) -> dataflowClosure env types graph expr (typeOf types expr) None
  | Let (Identifier name, optType, binder, body) ->
      let depsBinder, g1, binder' = dataflowE env types graph binder
      // FIXME: Is a node for the binder really needed?
      let binderNode, g2 = makeFinalNode env types g1 binder' depsBinder name
      
      // Add the binder to the environment to dataflow the body
      let env' = env.Add(name, binderNode).Add(binderNode.Uid, binderNode)
      let types' = types.Add(name, binderNode.Type).Add(binderNode.Uid, binderNode.Type)
      
      let depsBody, g3, body' = dataflowE env' types' g2 body
      Set.add binderNode depsBody, g3, Let (Identifier name, optType, Id (Identifier binderNode.Uid), body')
  | If (cond, thn, els) ->
      let deps1, g1, cond' = dataflowE env types graph cond
      let deps2, g2, thn' = dataflowE env types g1 thn
      let deps3, g3, els' = dataflowE env types g2 els
      Set.union (Set.union deps1 deps2) deps3, g3, If (cond', thn', els')
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
      | Some info' -> if info'.Type <> TyUnit
                        then Set.singleton info', graph, Id (Identifier info'.Uid)
                        else Set.empty, graph, expr // If the type is unknown, nothing depends on it.
      | _ -> raise (UnknownId name)
  | Integer i -> Set.empty, graph, expr
  | String s -> Set.empty, graph, expr
  | Bool b -> Set.empty, graph, expr
  | _ -> failwithf "Expression type not supported: %A" expr

and dataflowMethod env types graph (target:NodeInfo) methName paramExps expr =
  match target.Type with
  | TyStream fields ->
      match methName with
      | "last" -> dataflowAggregate env types graph target paramExps makeLast methName expr
      | "sum" -> dataflowAggregate env types graph target paramExps makeSum methName expr
      | "count" -> dataflowAggregate env types graph target paramExps makeCount methName expr
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
  | TyWindow (TyStream fields, TimedWindow _) ->
      match methName with
      | "last" -> dataflowAggregate env types graph target paramExps makeLast methName expr
      | "sum" -> dataflowAggregate env types graph target paramExps makeSum methName expr
      | "count" -> dataflowAggregate env types graph target paramExps makeCount methName expr
      | "groupby" -> dataflowGroupby env types graph target paramExps expr
      | _ -> failwithf "Unkown method of type Window: %s" methName
  | TyWindow (_, TimedWindow _) ->
      match methName with
      | "sum" -> dataflowAggregate env types graph target paramExps makeSum methName expr
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
      | _ -> failwithf "Unkown method: %s" methName
  | TyInt -> match methName with
             | "[]" -> let duration = match paramExps with
                                      | [Time (Integer v, unit) as t] -> toSeconds v unit
                                      | _ -> failwith "Invalid duration"
                       let n, g' = createNode (nextSymbol "[x min]") (TyWindow (target.Type, TimedWindow duration))
                                              [target] (makeDynValWindow duration) graph
                       Set.singleton n, g', Id (Identifier n.Uid)
             | "updated" -> let n, g' = createNode (nextSymbol "toStream") (TyStream (TyRecord (Map.of_list ["value", TyInt])))
                                                   [target] (makeToStream) graph
                            Set.singleton n, g', Id (Identifier n.Uid)
             | "sum" -> dataflowAggregate env types graph target paramExps makeSum methName expr
             | "count" -> dataflowAggregate env types graph target paramExps makeCount methName expr
             | _ -> failwithf "Unkown method: %s" methName
  | TyRecord _ -> match methName with
                  | "updated" -> let n, g' = createNode (nextSymbol "toStream") (TyStream (TyRecord (Map.of_list ["value", TyInt])))
                                                        [target] (makeToStream) graph
                                 Set.singleton n, g', Id (Identifier n.Uid)
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
      | [target; Lambda ([Param (Identifier arg, _)], handler)] ->
          // Dataflow the target
          let depsTarget, g1, target' = dataflowE env types graph target
          let depTarget = match Set.to_list depsTarget with
                          | [t] -> t
                          | _ -> failwith "OMG the target of the when is not a simple node!!!"
          // Dataflow the handler expression
          let env' = env.Add(arg, NodeInfo.AsUnknown(arg))
          let types' = types.Add(arg, TyUnit)
          let depsHandler, g2, handler' = dataflowE env' types' g1 handler
          // Create a node for the when operator
          let allDeps = depTarget::(Set.to_list depsHandler)
          let n, g3 = createNode (nextSymbol "when") TyUnit allDeps
                                 (makeWhen (Lambda ([Param (Identifier arg, None)], handler'))) g2
          Set.singleton n, g3, Id (Identifier n.Uid)
      | _ -> failwithf "Invalid parameters to when: %A" paramExps
  | Id (Identifier "$ref") ->
      match paramExps with
      | [expr] ->
          let deps', g1, expr' = dataflowE env types graph expr
          
          // Get the type of the referenced object and create a function that
          // will return its unique id
          let refType = match typeOf types expr with
                        | TyEntity name -> match types.[name] with
                                           | TyType (fields, id) -> id
                                           | _ -> failwithf "The type of an entity is not a TyType?!"
                        | _ -> failwithf "Only entities may be referenced"
          let getId = fun entity -> match entity with
                                    | VRecord m -> !m.[VString refType]
                                    | _ -> failwithf "The entity is not a record?!"
          let n, g2 = makeFinalNode env types g1 expr' deps' "{ }"
          let ref, g3 = createNode (nextSymbol "ref") (TyRef (typeOf types expr)) [n]
                                   (makeRef getId) g2
          Set.singleton ref, g3, Id (Identifier ref.Uid)
      | _ -> invalid_arg "paramExps"
  | Id (Identifier "print") -> dataflowByEval ()    
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
  
(*
// Dataflow a recursive function definition
and dataflowLetRec env types graph expr =
  let name, typ, binder, body =
    match expr with
    | Let (Identifier name, optType, binder, body) ->
        match optType with
        | Some typ -> name, typ, binder, body
        | None -> failwithf "Can't happen"
    | _ -> failwithf "Can't happen"

  // Dataflow the binder to extract global dependencies
  let binderDeps, g1, binder' = 
    match binder with
    | Lambda (args, body) ->
         // Add the arguments to the environment
         let env', types' =
           List.fold (fun (env:NodeContext, types:TypeContext) (Param (Identifier arg, _)) ->
                        let n = NodeInfo.AsUnknown(arg)
                        env.Add(arg, n), types.Add(arg, n.Type))
                     (env, types) args
         // Now add the recursive function
         let env'', types'' = env'.Add(name, NodeInfo.AsUnknown(name)), types'.Add(name, TyUnit)
         
         let deps, g1, body' = dataflowE env'' types'' graph body
         deps, g1, Lambda (args, body')
    | _ -> failwithf "The binder of a recursive let is not a lambda?!"

  // At this point there are two choices: does the body return a closure?
  // If it does, we have to manually create the necessary nodes with the
  // correct LambdaStates.
  match typeOf types expr with
  | TyArrow _ ->
      // Create the node for the binder
      let binderNode, g2 = createClosure env types g1 binder' typ (Some name)
      binderNode.Name <- name
      
      // Dataflow the body
      let env', types' = env.Add(name, binderNode), types.Add(name, binderNode.Type)
      let bodyDeps, g3, body' = dataflowE env' types' g2 body
      
      // Create a node for the body
      let bodyNode, g4 = makeFinalNode env' types' g3 body' bodyDeps "let-body"

      let expr' = Let (Identifier name, Some typ, Id (Identifier binderNode.Uid), Id (Identifier bodyNode.Uid))
      let n, g5 = createNode (nextSymbol "let") (typeOf types' expr) [binderNode; bodyNode]
                             (ForwardState bodyNode) (makeEvaluator expr' Map.empty) g4
      Set.singleton n, g5, Id (Identifier n.Uid)
  | _ -> // Dataflow the body
         let env', types' = env.Add(name, NodeInfo.AsUnknown(name)), types.Add(name, TyUnit)
         let bodyDeps, g2, body' = dataflowE env' types' g1 body
         Set.union binderDeps bodyDeps, g2, Let (Identifier name, Some typ, binder', body')
*)


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
    let action env types (graph:DataflowGraph) =
      let opResult, deps', g', subExprBuilder = dataflowSubExpr env types graph

      // Modify the node and add it to the graph
      let (p, uid, info, s) = graph.[nodeUid]
      let n = { info with MakeOper = opMaker subExprBuilder }
     
      let g'' = Graph.add (p, n.Uid, n, s) g'
      g''

    // Try the normal dataflow first. If there is an error, we add "action" as a continuation.
    retry (fun () -> dataflowE env types graph body)
          (fun err -> match err with
                      | IncompleteRecord ((deps, g', expr'), ids) ->
                          for id in ids do
                            theForwardDeps.Add(id, action)
                         
                          deps, g', expr'
                      | _ -> raise err)

  and dataflowSubExpr (env:NodeContext) (types:TypeContext) (graph:DataflowGraph) =
    let body, env', types', g1, arg =
      match paramExps with
      | [Lambda ([Param (Identifier arg, _)], body) as fn] ->
          let argInfo = { Uid = arg; Type = argType; MakeOper = argMaker; Name = arg; ParentUids = [] }
          body, env.Add(arg, argInfo), types.Add(arg, argType), graph.Add([], arg, argInfo, []), arg
      | _ -> invalid_arg "paramExps"
      
    extractSubNetwork env' types' g1 (keepTrying arg) [arg] body
                    
  let opResult, deps, g', subExprBuilder = dataflowSubExpr env types graph

  // If the body itself does not depend on the argument (e.g., t -> some-expression-without-t),
  // then it is a "constant" and we can simply mark it as a dependency of the operation.
  // To ensure it doesn't depend on the argument, check if it is present in g3.
  let opDeps = target::(Set.to_list (if Graph.contains opResult.Uid g' then Set.add opResult deps else deps))

  let n, g4 = createNode nodeUid (TyDict resType) opDeps
                         (opMaker subExprBuilder) g'
  Set.singleton n, g4, Id (Identifier n.Uid)

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
and extractSubNetwork env types graph dataflowBodyFn args body =
  let deps, g2, body' = dataflowBodyFn env types graph body

  // Make a final node for the evaluation of the body, if necessary
  let opResult, g2' = makeFinalNode env types g2 body' deps "{ }"

  // Remove from g2 everything that depends on the argument
  // and update the set of dependencies
  let deps', g3 = removeNetworks args deps g2'

  // Uses g2 because it contains the group's subnetwork
  opResult, deps', g3, makeSubExprBuilder args opResult.Uid g2'


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

  (* The result is an arbitrary expression and we need to evaluate it *)
  | _ -> createNode (nextSymbol name) (typeOf types' expr) (Set.to_list deps)
                    (makeEvaluator expr Map.empty) graph

and dataflowClosure env types graph expr typ (itself:string option) =
  match expr with 
  | Lambda (args, body) ->
      // Extract the body's network, so that we can recreate it everytime we
      // call the closure.
      let env', types', g1, argUids =
        List.fold (fun (env:NodeContext, types:TypeContext, graph, argUids) (Param (Identifier arg, typ)) ->
                     match typ with
                     | Some t -> let node = { Uid = arg; Type = t; MakeOper = makeInitialOp; Name = arg; ParentUids = [] }
                                 env.Add(arg, node), types.Add(arg, node.Type), Graph.add ([], arg, node, []) graph, argUids @ [node.Uid]
                     | None -> failwithf "Function arguments must have type annotations.")
                  (env, types, graph, []) args
                  
      let _, deps, g2, networkBuilder = extractSubNetwork env' types' g1 dataflowE argUids body
      // FIXME: Compare with dataflowdictOps. Do I need to check if opResult is in g2 and add it as a dep?

      let ids = (List.fold (fun acc (Param (Identifier id, _)) -> acc + id + ".") "" args)
      let uid = sprintf "λ%s" ids
      let n, g3 = createNode (nextSymbol uid) typ (Set.to_list deps)
                             (makeClosure networkBuilder) g2           
      Set.singleton n, g3, Id (Identifier n.Uid)
  | _ -> failwithf "Not a lambda"

and dataflowAggregate env types graph target paramExprs opMaker aggrName expr =
  let field = match paramExprs with
              | [SymbolExpr (Symbol name)] -> name
              | _ -> ""
  let getMaker name = function
    | VEvent ev -> ev.[name]
    | other -> failwithf "getMaker expects events but was called with a %A" other
                   
  let getField = match paramExprs, target.Type with
                 | [SymbolExpr (Symbol name)], TyStream _ -> getMaker name
                 | [SymbolExpr (Symbol name)], TyWindow (TyStream _, TimedWindow _) -> getMaker name
                 | [], TyInt -> id
                 | [], TyWindow (TyInt, TimedWindow _) -> id
                 | _ -> failwith "Invalid parameters to %s" aggrName
  let n, g' = createNode (nextSymbol aggrName) TyInt [target]
                         (opMaker getField) graph
  n.Name <- (sprintf "%s(%s)" aggrName field)
  Set.singleton n, g', Id (Identifier n.Uid)

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
 * Handles stream.select()
 *)
and dataflowSelect env types graph target paramExps expr =
  let subExpr, env', types', arg =
    match paramExps with
    | [Lambda ([Param (Identifier arg, _)], body) as fn] ->
        body,
        // Put the argument as an Unknown node into the environment
        // This way it will be ignored by the dataflow algorithm
        Map.add arg (NodeInfo.AsUnknown(arg)) env,                    // THIS IS WRONG. WE KNOW THE CORRECT TYPE!!!
        Map.add arg TyUnit types,
        arg
    | _ -> failwith "Invalid parameter to where"
  let deps, g', expr' = dataflowE env' types' graph subExpr
  let expr'' = Lambda ([Param (Identifier arg, None)], expr')
 
  // Find the type of the resulting stream
  let inEvType = match target.Type with
                 | TyStream evType -> evType
                 | _ -> failwithf "stream.select can only be applied to streams"
  let resultType = match typeOf (types.Add(arg, inEvType)) expr' with
                   | TyRecord fields -> TyStream (TyRecord (fields.Add("timestamp", TyInt)))
                   | _ -> failwithf "stream.select must return an event"
 
  // Depends on the stream and on the dependencies of the predicate.
  let opDeps = target::(Set.to_list deps)
  let n, g'' = createNode (nextSymbol "Select") resultType opDeps
                          (makeSelect expr'') g'
  Set.singleton n, g'', Id (Identifier n.Uid)
  
  
(*
 * Handles stream.where()
 *)
and dataflowWhere env types graph target paramExps expr =
  let subExpr, env', types', arg =
    match paramExps with
    | [Lambda ([Param (Identifier arg, _)], body) as fn] ->
        body,
        // Put the argument as an Unknown node into the environment
        // This way it will be ignored by the dataflow algorithm
        Map.add arg (NodeInfo.AsUnknown(arg)) env,                    // THIS IS WRONG. WE KNOW THE CORRECT TYPE!!!
        Map.add arg TyUnit types,
        arg
    | _ -> failwith "Invalid parameter to where"
  let deps, g', expr' = dataflowE env' types' graph subExpr
  let expr'' = Lambda ([Param (Identifier arg, None)], expr')
 
  // Depends on the stream and on the dependencies of the predicate.
  let opDeps = target::(Set.to_list deps)
  let n, g'' = createNode (nextSymbol "Where") target.Type opDeps
                          (makeWhere expr'') g'
  Set.singleton n, g'', Id (Identifier n.Uid)
  
  
(*
 * Iterate the graph and create the operators and the connections between them.
 *)
and makeOperNetwork (graph:DataflowGraph) (roots:string list) fixPrio context : Map<string, Operator> =
  // Spread all changes in the stack. If some step fails, try the remaining steps.
  let rec retrySpread stack =
      try
        spread stack
      with
        | SpreadException (_, rest, _) -> retrySpread rest
        
  let spreadInitialChanges initialChanges =
    for (op:Operator, (changes:changes)) in initialChanges do
      op.AllChanges := [changes]
    let stack = List.fold (fun stack (op, changes) -> mergeStack stack (toEvalStack op.Children changes)) [] initialChanges
    retrySpread stack

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
                            let changes' = if op.Value <> VNull then (op, [Added (op.Value.Clone())])::changes else changes
                            //printfn "Created operator %s with priority %A" uid (fixPrio orderPrio.[uid])
                            Map.add uid op context, changes')
                  (context, []) graph order
                  
  // This magical line will set the Context field of all the operators created
  // in this iteration to point to the same map.                  
  contextRef := context'          
  spreadInitialChanges initialChanges
  context'               