#light

open System
open System.Text.RegularExpressions
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

type uid = string
type NodeInfo =
  { Uid:uid
    Type:Type
    mutable Name:string
    // uid -> priority -> parents -> created operator
    MakeOper:uid -> Priority.priority -> Operator list -> Operator
    ParentUids:uid list }

    interface IComparable with
      member self.CompareTo(other) =
        match other with
        | :? NodeInfo as other' -> String.compare self.Uid other'.Uid
        | _ -> failwith "Go away. Other is not even a NodeInfo."

    override self.Equals(other) =
      match other with
        | :? NodeInfo as other' -> self.Uid = other'.Uid
        | _ -> false

    (* Creates a node with type "Unknown", no parents and an evaluation
       function that will never be called.
       Unknown nodes are ignored by the dataflow algorithm *)
    static member AsUnknown(uid) =
      let makeOper' = fun _ _ _ -> failwith "Won't happen!"

      { Uid = uid; Type = TyUnit; Name = uid; MakeOper = makeOper';
        ParentUids = [] }

    static member UidOf(nodeInfo) = nodeInfo.Uid

type DataflowGraph = Graph<string, NodeInfo>
type NodeContext = Map<string, NodeInfo>

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
  
  let add uid action map : ForwardDepsRep =
    Map.add uid (if Map.mem uid map
                   then action::map.[uid]
                   else [action])
                map
                
  let mem = Map.mem
  let empty = Map.empty
  let is_empty = Map.is_empty
  let resolve name (map:ForwardDepsRep) env types graph =
    List.fold_left (fun g a -> a env types g) graph map.[name]
  

let nextSymbol =
  let counter = ref 0
  (fun prefix -> counter := !counter + 1
                 prefix + (!counter).ToString())

let uid2name uid = Regex.Match(uid, "(.*)\d").Groups.[1].Value

(*
 * Creates a new node and adds it to the graph.
 * Returns both the node and the modified graph.
 *)
let createNode uid typ parents makeOp graph =
  let parentUids = List.map NodeInfo.UidOf parents
  let node = { Uid = uid; Type = typ; Name = uid2name uid;
               MakeOper = makeOp; ParentUids = parentUids }
  node, Graph.add (parentUids, uid, node, []) graph

exception UnknownId of string

let rec dataflow (env:NodeContext, types:TypeContext, (graph:DataflowGraph), roots, k) = function
  | Def (Identifier name, exp) ->
      let deps, g', expr', k' = dataflowE env types graph exp k
      let roots' = name::roots
      let n, g'' = makeFinalNode env types g' expr' deps name

      // Sanity check
      //if n.Type <> types.[name] then failwithf "Different types for %s! %A <> %A" name n.Type types.[name]
      let env' = env.Add(name, n).Add(n.Uid, n)
      let types' = types.Add(n.Uid, n.Type)
      
      let g3 = if ForwardDeps.mem name k'
                 then ForwardDeps.resolve name k' env' types' g''
                 else g''
        
      env', types', g3, roots', k'
  | Expr expr ->
      let deps, g', expr', k' = dataflowE env types graph expr k
      env, types, g', roots, k'


and dataflowE (env:NodeContext) types (graph:DataflowGraph) expr k =   
  match expr with
  | MethodCall (target, (Identifier name), paramExps) ->
      let deps, g1, target', k1 = dataflowE env types graph target k
      let n, g2 = makeFinalNode env types g1 target' deps ("<X>." + name + "()")
      let deps', g3, expr', k2 = dataflowMethod env types g2 n name paramExps k1
      deps', g3, expr', k2
  | ArrayIndex (target, index) ->
      match index with
      | Time (_, _) -> dataflowE env types graph (MethodCall (target, Identifier "[]", [index])) k
      | _ -> let depsTrg, g', target', k' = dataflowE env types graph target k
             let depsIdx, g'', index', k'' = dataflowE env types g' index k'
             Set.union depsTrg depsIdx, g'', ArrayIndex (target', index'), k''
  | FuncCall (fn, paramExps) -> dataflowFuncCall env types graph fn paramExps expr k
  | MemberAccess (expr, (Identifier name)) ->
      let deps, g', expr', k' = dataflowE env types graph expr k
      deps, g', MemberAccess (expr', (Identifier name)), k'
  | Record fields ->
      let deps, g', exprs, k' =
        List.fold_left (fun (depsAcc, g, exprsAcc, k) (Symbol field, expr) ->
                          let deps, g', expr', k' = dataflowE env types g expr k
                          Set.union depsAcc deps, g', exprsAcc @ [Symbol field, expr'], k')       // Is the order relevant?
                       (Set.empty, graph, [], k) fields
      deps, g', Record exprs, k'
  | Let (Identifier name, binder, body) ->
      let deps1, g1, binder', k1 = dataflowE env types graph binder k
      let env' = env.Add(name, NodeInfo.AsUnknown(name))                                      // FIXME: Create a final node for deps1 and use it?
      let deps2, g2, body', k2 = dataflowE env' types g1 body k1
      Set.union deps1 deps2, g2, Let (Identifier name, binder', body'), k2
  | If (cond, thn, els) ->
      let deps1, g1, cond', k1 = dataflowE env types graph cond k
      let deps2, g2, thn', k2 = dataflowE env types g1 thn k1
      let deps3, g3, els', k3 = dataflowE env types g2 els k2
      Set.union (Set.union deps1 deps2) deps3, g3, If (cond', thn', els'), k3
  | BinaryExpr (oper, expr1, expr2) as expr ->
      let deps1, g1, expr1', k1 = dataflowE env types graph expr1 k
      let deps2, g2, expr2', k2 = dataflowE env types g1 expr2 k1
      Set.union deps1 deps2, g2, BinaryExpr (oper, expr1', expr2'), k2
  | Seq (expr1, expr2) ->
      let deps1, g1, expr1', k1 = dataflowE env types graph expr1 k
      let deps2, g2, expr2', k2 = dataflowE env types g1 expr2 k1
      Set.union deps1 deps2, g2, Seq (expr1', expr2'), k2
  | Id (Identifier name) ->
      let info = Map.tryfind name env
      match info with
      | Some info' -> if info'.Type <> TyUnit
                        then Set.singleton info', graph, Id (Identifier info'.Uid), k
                        else Set.empty, graph, expr, k // If the type is unknown, nothing depends on it.
      | _ -> raise (UnknownId name)
  | Integer i -> Set.empty, graph, expr, k
  | String s -> Set.empty, graph, expr, k
  | Bool b -> Set.empty, graph, expr, k
  | _ -> failwithf "Expression type not supported: %A" expr

and dataflowMethod env types graph (target:NodeInfo) methName paramExps k =
  match target.Type with
  | TyStream fields ->
      match methName with
      | "last" -> dataflowAggregate graph target paramExps makeLast methName k
      | "sum" -> dataflowAggregate graph target paramExps makeSum methName k
      | "count" -> dataflowAggregate graph target paramExps makeCount methName k
      | "[]" -> let duration = match paramExps with
                               | [Time (Integer v, unit) as t] -> toSeconds v unit
                               | _ -> failwith "Invalid duration"
                let n, g' = createNode (nextSymbol "[x min]") (TyWindow (target.Type, TimedWindow duration)) [target]
                                       (makeWindow duration) graph
                Set.singleton n, g', Id (Identifier n.Uid), k
      | "where" -> dataflowWhere env types graph target paramExps k
      | "select" -> dataflowSelect env types graph target paramExps k
      | "groupby" -> dataflowGroupby env types graph target paramExps k
      | _ -> failwithf "Unkown method: %s" methName
  | TyWindow (TyStream fields, TimedWindow _) ->
      match methName with
      | "last" -> dataflowAggregate graph target paramExps makeLast methName k
      | "sum" -> dataflowAggregate graph target paramExps makeSum methName k
      | "count" -> dataflowAggregate graph target paramExps makeCount methName k
      | "groupby" -> dataflowGroupby env types graph target paramExps k
      | _ -> failwithf "Unkown method of type Window: %s" methName
  | TyWindow (_, TimedWindow _) ->
      match methName with
      | "sum" -> dataflowAggregate graph target paramExps makeSum methName  k
      | _ -> failwithf "Unkown method of type Window: %s" methName
  | TyDict valueType ->
      match methName with
      | "where" -> dataflowDictOps env types graph target paramExps valueType makeInitialOp valueType makeDictWhere methName k
      | "select" ->
          let arg, body =
            match paramExps with
            | [Lambda ([Identifier arg], body) as fn] ->
                arg, body
            | _ -> failwith "Invalid parameter to dict/select"
          let projType = typeOf (types.Add(arg, valueType)) body
          dataflowDictOps env types graph target paramExps valueType makeInitialOp projType makeDictSelect methName k
      | _ -> failwithf "Unkown method: %s" methName
  | TyInt -> match methName with
             | "[]" -> let duration = match paramExps with
                                      | [Time (Integer v, unit) as t] -> toSeconds v unit
                                      | _ -> failwith "Invalid duration"
                       let n, g' = createNode (nextSymbol "[x min]") (TyWindow (target.Type, TimedWindow duration)) [target]
                                              (makeDynValWindow duration) graph
                       Set.singleton n, g', Id (Identifier n.Uid), k
             | "updated" -> let n, g' = createNode (nextSymbol "toStream") (TyStream (TyRecord (Map.of_list ["value", TyInt]))) [target]
                                                   (makeToStream) graph
                            Set.singleton n, g', Id (Identifier n.Uid), k
             | "sum" -> dataflowAggregate graph target paramExps makeSum methName k
             | "count" -> dataflowAggregate graph target paramExps makeCount methName k
             | _ -> failwithf "Unkown method: %s" methName
  | TyRecord _ -> match methName with
                  | "updated" -> let n, g' = createNode (nextSymbol "toStream") (TyStream (TyRecord (Map.of_list ["value", TyInt]))) [target]
                                                        (makeToStream) graph
                                 Set.singleton n, g', Id (Identifier n.Uid), k
                  | _ -> failwithf "Unkown method of type Record: %s" methName
  | _ -> failwith "Unknown target type"

and dataflowFuncCall env types graph fn paramExps expr k =
  match fn with
  | Id (Identifier "stream") ->
      let n, g' = createNode (nextSymbol "stream") (typeOf Map.empty expr) [] makeStream graph
      Set.singleton n, g', Id (Identifier n.Uid), k
  | Id (Identifier "when") ->
      match paramExps with
      | [target; Lambda ([Identifier arg], handler)] ->
          // Dataflow the target
          let depsTarget, g', target', k' = dataflowE env types graph target k
          let depTarget = match Set.to_list depsTarget with
                          | [t] -> t
                          | _ -> failwith "OMG the target of the when is not a simple node!!!"
          // Dataflow the handler expression
          let env' = env.Add(arg, NodeInfo.AsUnknown(arg))
          let types' = types.Add(arg, TyUnit)
          let depsHandler, g'', handler', k'' = dataflowE env' types' g' handler k'
          // Create a node for the when operator
          let allDeps = depTarget::(Set.to_list depsHandler)
          let n, g''' = createNode (nextSymbol "when") TyUnit allDeps
                                   (makeWhen (Lambda ([Identifier arg], handler'))) g''
          Set.singleton n, g''', Id (Identifier n.Uid), k''
      | _ -> failwithf "Invalid parameters to when: %A" paramExps
  | _ -> let deps, g', paramExps', k' = List.fold_left (fun (depsAcc, g, exprs, k) expr ->
                                                          let deps, g', expr', k' = dataflowE env types g expr k
                                                          Set.union depsAcc deps, g', exprs @ [expr'], k')
                                                       (Set.empty, graph, List.empty, k) paramExps
         deps, g', FuncCall (fn, paramExps'), k'

and dataflowGroupby env types graph target paramExps k =
  let argType = target.Type
  let argMaker = match argType with
                 | TyStream _ -> makeStream
                 | TyWindow (TyStream _, TimedWindow _) -> makeSimpleWindow
  let field, body, arg =
    match paramExps with
    | [SymbolExpr (Symbol field); Lambda ([Identifier arg], body) as fn] ->
        let argInfo = { Uid = arg; Type = argType; MakeOper = argMaker; Name = arg; ParentUids = [] }
        field, body, arg
    | _ -> failwith "Invalid parameter to groupby"
  let valueType = typeOf (types.Add(arg, argType)) body
    
  dataflowDictOps env types graph target [Lambda ([Identifier arg], body)]
                  argType argMaker valueType (makeGroupby field) "groupBy" k

(*
 * Dataflows stream.groupby(), dict.select() and dict.where().
 *)
and dataflowDictOps env types graph target paramExps argType argMaker resType opMaker methName k =
  let body, env', types', g1, arg =
    match paramExps with
    | [Lambda ([Identifier arg], body) as fn] ->
        let argInfo = { Uid = arg; Type = argType; MakeOper = argMaker; Name = arg; ParentUids = [] }
        body, env.Add(arg, argInfo), types.Add(arg, argType), graph.Add([], arg, argInfo, []), arg
  let deps, g2, body', k' = dataflowE env' types' g1 body k

  // Make a final node for the evaluation of the body, if necessary
  let opResult, g2' = makeFinalNode env' types' g2 body' deps "op-result"

  // Remove from g2 everything that depends on the argument
  // and update the set of dependencies
  let deps', g3 = removeNetwork arg deps g2'
                    
  // If the body itself does not depend on the argument (e.g., t -> some-expression-without-t),
  // then it is a "constant" and we can simply mark it as a dependency of the operation.
  // To ensure it doesn't depend on the argument, check if it is present in g3.
  let opDeps = target::(Set.to_list (if Graph.mem opResult.Uid g3 then Set.add opResult deps' else deps'))
 
  // Uses g2 because it contains the group's subnetwork
  let projBuilder = makeSubExprBuilder arg opResult.Uid body' deps' env' types' g2'
  let n, g4 = createNode (nextSymbol methName) (TyDict resType) opDeps
                         (opMaker projBuilder) g3
  Set.singleton n, g4, Id (Identifier n.Uid), k' 

and makeSubExprBuilder initial final body deps env types graph =
  fun prio operators ->
    let fixPrio p = Priority.add (Priority.down prio) p
    let operators' = makeOperNetwork graph [initial] fixPrio operators
    operators'.[initial], operators'.[final]

(* Create the necessary nodes to evaluate an expression.
 *  - If the expression is a simple variable access, no new nodes are necessary;
 *  - If the expression is a record, we will need new nodes for each field and
 *    another node for the entire record;
 *  - If the expression is of any other kind, we create a general purpose
 *    evaluator node.
 *)
and makeFinalNode env types graph expr deps name =
  (* Extend the environment to include dependencies of all the subexpressions *)
  let env' = Set.fold_left (fun acc info -> Map.add info.Uid info acc) env deps
  let types' = Set.fold_left (fun acc info -> Map.add info.Uid info.Type acc) types deps
      
  match expr with
  (* No additional node necessary *)
  | Id (Identifier uid) -> Set.choose deps, graph
 
  (* Yes, the result is a record and we need the corresponding operator *)
  | Record fields ->
      let g'', fieldDeps =
        List.fold_left (fun (g, fieldDeps) (Symbol field, expr) ->
                          (* Lets get the dependencies for just this expression *)
                          let deps'', g', expr', k' = dataflowE env' types' g expr ForwardDeps.empty
                          
                          (* Sanity check: at this point, the expression should be completely
                             "continualized" and thus, the previous operation must not have
                             altered the graph nor the expression itself *)
                          assert (g' = g && expr' = expr)
                           
                          (* Do we need an evaluator just for this field? *)
                          let n, g' = makeFinalNode env' types' g expr deps'' field
                          g', (field, n)::fieldDeps)
                       (graph, []) fields

      let uids = List.map snd fieldDeps
      let fieldTypes = Map.of_list (List.map (fun (f, n) -> (f, n.Type)) fieldDeps)                                  
      createNode (nextSymbol name) (TyRecord fieldTypes) uids
                 // At runtime, we need to find the operators corresponding to the parents
                 (fun uid prio parents ->
                    let parentOps = List.map (fun (f, n) ->
                                                (f, List.find (fun (p:Operator) -> p.Uid = n.Uid) parents))
                                             fieldDeps
                    makeRecord parentOps uid prio parents)
                 g''

  (* The result is an arbitrary expression and we need to evaluate it *)
  | _ -> createNode (nextSymbol name) (typeOf types' expr) (Set.to_list deps) (makeEvaluator expr) graph

and dataflowAggregate graph target paramExprs opMaker aggrName k =
  let getField = match paramExprs, target.Type with
                 | [SymbolExpr (Symbol name)], TyStream _ -> (fun (VEvent ev) -> ev.[name])
                 | [SymbolExpr (Symbol name)], TyWindow (TyStream _, TimedWindow _) -> (fun (VEvent ev) -> ev.[name])
                 | [], TyInt -> id
                 | [], TyWindow (TyInt, TimedWindow _) -> id
                 | _ -> failwith "Invalid parameters to %s" aggrName
  let n, g' = createNode (nextSymbol aggrName) TyInt [target]
                         (opMaker getField) graph
  Set.singleton n, g', Id (Identifier n.Uid), k

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
      List.fold_left (fun (deps, g) s -> removeNetwork s deps g) (deps', g') s
  | _ -> deps, graph



(*
  let rec keepTrying env types graph expr k =
    retry (fun () -> dataflowE env types graph expr k)
          (fun err -> match err with
                      | UnknownId name ->
                          match typeOf types (Id (Identifier name)) with
                          | TyDict (TyRecord _) -> 
                              let env' = env.Add(name, NodeInfo.AsUnknown(name))
                              let action (env:NodeContext) (types:TypeContext) graph =
                                // Add g to this new environment
                                let env' = env.Add(arg, NodeInfo.AsUnknown(arg))
                                let types' = types.Add(arg, TyUnit)
                                
                                let deps, g', expr', isDone, k' = dataflowE env' types' graph expr ForwardDeps.empty
                                assert (ForwardDeps.is_empty k' && not isDone)
                                
                                let groupBuilder = makeSubExprBuilder (arg, argType, argMaker) expr' deps env' types' g'
                                let n = Graph.labelOf nodeUid graph
                                let n' = { n with MakeOper = makeGroupby field groupBuilder }
                                Graph.add (n'.ParentUids, n'.Uid, n', []) graph
                                
                              let k' = ForwardDeps.add name action k
                              keepTrying env' types graph expr k'
                          | _ -> raise err
                      | _ -> raise err)

  let deps, g', expr', _, k' = keepTrying env' types' graph expr k
  *)

(*
 * Handles stream.select()
 *)
and dataflowSelect env types graph target paramExps k =
  let subExpr, env', types', arg =
    match paramExps with
    | [Lambda ([Identifier arg], body) as fn] ->
        body,
        // Put the argument as an Unknown node into the environment
        // This way it will be ignored by the dataflow algorithm
        Map.add arg (NodeInfo.AsUnknown(arg)) env,                    // THIS IS WRONG. WE KNOW THE CORRECT TYPE!!!
        Map.add arg TyUnit types,
        arg
    | _ -> failwith "Invalid parameter to where"
  let deps, g', expr', k' = dataflowE env' types' graph subExpr k
  let expr'' = Lambda ([Identifier arg], expr')
 
  // Find the type of the resulting stream
  let inEvType = match target.Type with
                 | TyStream evType -> evType
  let resultType = TyStream (typeOf (types.Add(arg, inEvType)) expr')
 
  // Depends on the stream and on the dependencies of the predicate.
  let opDeps = target::(Set.to_list deps)
  let n, g'' = createNode (nextSymbol "Select") resultType opDeps
                          (makeSelect expr'') g'
  Set.singleton n, g'', Id (Identifier n.Uid), k'
  
  
(*
 * Handles stream.where()
 *)
and dataflowWhere env types graph target paramExps k =
  let subExpr, env', types', arg =
    match paramExps with
    | [Lambda ([Identifier arg], body) as fn] ->
        body,
        // Put the argument as an Unknown node into the environment
        // This way it will be ignored by the dataflow algorithm
        Map.add arg (NodeInfo.AsUnknown(arg)) env,                    // THIS IS WRONG. WE KNOW THE CORRECT TYPE!!!
        Map.add arg TyUnit types,
        arg
    | _ -> failwith "Invalid parameter to where"
  let deps, g', expr', k' = dataflowE env' types' graph subExpr k
  let expr'' = Lambda ([Identifier arg], expr')
 
  // Depends on the stream and on the dependencies of the predicate.
  let opDeps = target::(Set.to_list deps)
  let n, g'' = createNode (nextSymbol "Where") target.Type opDeps
                          (makeWhere expr'') g'
  Set.singleton n, g'', Id (Identifier n.Uid), k'
  
  
(*
 * Iterate the graph and create the operators and the connections between them.
 *)
and makeOperNetwork (graph:DataflowGraph) (roots:string list) fixPrio operators : Map<string, Operator> =
  let order = Graph.Algorithms.topSort roots graph
  
  // Maps uids to priorities
  let orderPrio = List.fold_left (fun (acc, prio) x -> (Map.add x prio acc, Priority.next prio))
                                 (Map.empty, Priority.initial) order |> fst

  // Fold the graph with the right order, returning a map of all the operators
  Graph.foldSeq (fun operators (pred, uid, info, succ) ->
                   // If the operator already exists, skip it.
                   if Map.mem uid operators
                     then operators
                     else let parents = List.map (fun uid -> Map.find uid operators) info.ParentUids
                          let op = info.MakeOper uid (fixPrio orderPrio.[uid]) parents
                          //printfn "Created operator %s with priority %A" uid (fixPrio orderPrio.[uid])
                          Map.add uid op operators)
                operators graph order

