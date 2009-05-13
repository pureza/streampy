#light

open System
open System.Text.RegularExpressions
open Ast
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

(*
 * Converts an environment into a type context to be used by the type checker.
 *)
let envTypes = Map.mapi (fun k v -> v.Type)

type NodeContext = Map<string, NodeInfo>

let rec dataflow (env:NodeContext, (graph:DataflowGraph), roots) = function
  | Def (Identifier name, exp) ->
      let deps, g', expr', _ = dataflowE env graph exp
      let roots' = name::roots
      let n, g'' = makeFinalNode env g' expr' deps name
      env.Add(name, n).Add(n.Uid, n), g'', roots'
  | Expr expr ->
      let deps, g', expr', _ = dataflowE env graph expr
      env, g', roots


and dataflowE (env:NodeContext) (graph:DataflowGraph) expr =   
  match expr with
  | MethodCall (target, (Identifier name), paramExps) ->
      let deps, g1, target', isDone = dataflowE env graph target
      if isDone
        then let n, g2 = makeFinalNode env g1 target' deps ("<X>." + name + "()")
             let deps', g3, expr' = dataflowMethod env g2 n name paramExps
             deps', g3, expr', true
        else deps, g1, MethodCall (target', (Identifier name), paramExps), false
  | ArrayIndex (target, index) ->
      match index with
      | Time (_, _) -> dataflowE env graph (MethodCall (target, Identifier "[]", [index]))
      | _ -> let depsTrg, g', target', isDoneTrg = dataflowE env graph target
             let depsIdx, g'', index', isDoneIdx = dataflowE env g' index
             Set.union depsTrg depsIdx, g'', ArrayIndex (target', index'), isDoneTrg && isDoneIdx
  | FuncCall (fn, paramExps) -> dataflowFuncCall env graph fn paramExps expr
  | MemberAccess (expr, (Identifier name)) ->
      let deps, g', expr', isDone = dataflowE env graph expr
      deps, g', MemberAccess (expr', (Identifier name)), isDone
  | Record fields ->
      let deps, g', exprs, isDone =
        List.fold_left (fun (depsAcc, g, exprsAcc, allDone) (Symbol field, expr) ->
                          let deps, g', expr', isDone = dataflowE env g expr
                          Set.union depsAcc deps, g', exprsAcc @ [Symbol field, expr'], allDone && isDone)       // Is the order relevant?
                       (Set.empty, graph, [], true) fields
      deps, g', Record exprs, isDone
  | Let (Identifier name, binder, body) ->
      let deps1, g1, binder', isDone1 = dataflowE env graph binder
      let env' = env.Add(name, NodeInfo.AsUnknown(name))                                      // FIXME: Create a final node for deps1 and use it?
      let deps2, g2, body', isDone2 = dataflowE env' g1 body
      Set.union deps1 deps2, g2, Let (Identifier name, binder', body'), isDone1 && isDone2
  | If (cond, thn, els) ->
      let deps1, g1, cond', isDone1 = dataflowE env graph cond
      let deps2, g2, thn', isDone2 = dataflowE env g1 thn
      let deps3, g3, els', isDone3 = dataflowE env g2 els
      Set.union (Set.union deps1 deps2) deps3, g3, If (cond', thn', els'), isDone1 && isDone2 && isDone3
  | BinaryExpr (oper, expr1, expr2) as expr ->
      let deps1, g1, expr1', isDone1 = dataflowE env graph expr1
      let deps2, g2, expr2', isDone2 = dataflowE env g1 expr2
      Set.union deps1 deps2, g2, BinaryExpr (oper, expr1', expr2'), isDone1 && isDone2
  | Seq (expr1, expr2) ->
      let deps1, g1, expr1', isDone1 = dataflowE env graph expr1
      let deps2, g2, expr2', isDone2 = dataflowE env g1 expr2
      Set.union deps1 deps2, g2, Seq (expr1', expr2'), isDone1 && isDone2
  | Id (Identifier name) ->
      let info = Map.tryfind name env
      match info with
      | Some info' -> if info'.Type <> TyUnit
                        then Set.singleton info', graph, Id (Identifier info'.Uid), true
                        else Set.empty, graph, expr, false // If the type is unknown, nothing depends on it.
      | _ -> failwithf "Identifier not found in the environment: %s" name
  | Integer i -> Set.empty, graph, expr, true
  | String s -> Set.empty, graph, expr, true
  | Bool b -> Set.empty, graph, expr, true
  | _ -> failwithf "Expression type not supported: %A" expr

and dataflowMethod env graph (target:NodeInfo) methName paramExps =
  match target.Type with
  | TyStream fields ->
      match methName with
      | "last" -> dataflowAggregate graph target paramExps makeLast methName
      | "sum" -> dataflowAggregate graph target paramExps makeSum methName
      | "count" -> dataflowAggregate graph target paramExps makeCount methName
      | "[]" -> let duration = match paramExps with
                               | [Time (Integer v, unit) as t] -> toSeconds v unit
                               | _ -> failwith "Invalid duration"
                let n, g' = createNode (nextSymbol "[x min]") (TyWindow (target.Type, TimedWindow duration)) [target]
                                       (makeWindow duration) graph
                Set.singleton n, g', Id (Identifier n.Uid)
      | "where" -> dataflowSelectWhere env graph target paramExps makeWhere methName 
      | "select" -> dataflowSelectWhere env graph target paramExps makeSelect methName 
      | "groupby" -> dataflowGroupby env graph target paramExps
      | _ -> failwithf "Unkown method: %s" methName
  | TyWindow (TyStream fields, TimedWindow _) ->
      match methName with
      | "last" -> dataflowAggregate graph target paramExps makeLast methName
      | "sum" -> dataflowAggregate graph target paramExps makeSum methName
      | "count" -> dataflowAggregate graph target paramExps makeCount methName
      | "groupby" -> dataflowGroupby env graph target paramExps
      | _ -> failwithf "Unkown method of type Window: %s" methName
  | TyWindow (_, TimedWindow _) ->
      match methName with
      | "sum" -> dataflowAggregate graph target paramExps makeSum methName
      | _ -> failwithf "Unkown method of type Window: %s" methName
  | TyDict valueType ->
      match methName with
      | "where" -> let expr, env', arg =
                     match paramExps with
                     | [Lambda ([Identifier arg], body) as fn] ->
                         body,
                         // Put the argument as an Unknown node into the environment
                         // This way it will be ignored by the dataflow algorithm
                         Map.add arg (NodeInfo.AsUnknown(arg)) env,
                         arg
                     | _ -> failwith "Invalid parameter to dict/where"
                   let deps, g', expr', _ = dataflowE env' graph expr
                   let expr'' = Lambda ([Identifier arg], expr')
                   let predBuilder = makeSubExprBuilder (arg, valueType, makeInitialOp) expr' deps
                   // Where depends on the parent dictionary and on the dependencies of the predicate.
                   let whereDeps = target::(Set.to_list deps)
                   let n, g'' = createNode (nextSymbol methName) target.Type whereDeps
                                           (makeDictWhere predBuilder) g'
                   Set.singleton n, g'', Id (Identifier n.Uid)
      | "select" -> let expr, env', arg =
                      match paramExps with
                      | [Lambda ([Identifier arg], body) as fn] ->
                          body,
                          // Put the argument as an Unknown node into the environment
                          // This way it will be ignored by the dataflow algorithm
                          Map.add arg (NodeInfo.AsUnknown(arg)) env,
                          arg
                      | _ -> failwith "Invalid parameter to dict/select"
                    let deps, g', expr', _ = dataflowE env' graph expr
                    let expr'' = Lambda ([Identifier arg], expr')
                    let projBuilder = makeSubExprBuilder (arg, valueType, makeInitialOp) expr' deps
                    // Select depends on the parent dictionary and on the dependencies of the predicate.
                    let selectDeps = target::(Set.to_list deps)
                    
                    // Note that env' contains arg with type Unknown, but we know its real type is valueType.
                    let valueType' = typeOf ((envTypes env).Add(arg, valueType)) expr
                    let n, g'' = createNode (nextSymbol methName) (TyDict valueType') selectDeps
                                            (makeDictSelect projBuilder) g'
                    Set.singleton n, g'', Id (Identifier n.Uid)
      | _ -> failwithf "Unkown method: %s" methName
  | TyInt -> match methName with
             | "[]" -> let duration = match paramExps with
                                      | [Time (Integer v, unit) as t] -> toSeconds v unit
                                      | _ -> failwith "Invalid duration"
                       let n, g' = createNode (nextSymbol "[x min]") (TyWindow (target.Type, TimedWindow duration)) [target]
                                              (makeDynValWindow duration) graph
                       Set.singleton n, g', Id (Identifier n.Uid)
             | "updated" -> let n, g' = createNode (nextSymbol "toStream") (TyStream ["value"]) [target]
                                                   (makeToStream) graph
                            Set.singleton n, g', Id (Identifier n.Uid)
             | "sum" -> dataflowAggregate graph target paramExps makeSum methName
             | "count" -> dataflowAggregate graph target paramExps makeCount methName
             | _ -> failwithf "Unkown method: %s" methName
  | TyRecord _ -> match methName with
                  | "updated" -> let n, g' = createNode (nextSymbol "toStream") (TyStream ["value"]) [target]
                                                        (makeToStream) graph
                                 Set.singleton n, g', Id (Identifier n.Uid)
                  | _ -> failwithf "Unkown method of type Record: %s" methName
  | _ -> failwith "Unknown target type"

and dataflowFuncCall env graph fn paramExps expr =
  match fn with
  | Id (Identifier "stream") ->
      let n, g' = createNode (nextSymbol "stream") (typeOf Map.empty expr) [] makeStream graph
      Set.singleton n, g', Id (Identifier n.Uid), true
  | Id (Identifier "when") ->
      match paramExps with
      | [target; Lambda ([Identifier arg], handler)] ->
          // Dataflow the target
          let depsTarget, g', target', _ = dataflowE env graph target
          let depTarget = match Set.to_list depsTarget with
                          | [t] -> t
                          | _ -> failwith "OMG the target of the when is not a simple node!!!"
          // Dataflow the handler expression
          let env' = env.Add(arg, NodeInfo.AsUnknown(arg))
          let depsHandler, g'', handler', _ = dataflowE env' g' handler
          // Create a node for the when operator
          let allDeps = depTarget::(Set.to_list depsHandler)
          let n, g''' = createNode (nextSymbol "when") TyUnit allDeps
                                   (makeWhen (Lambda ([Identifier arg], handler'))) g''
          Set.singleton n, g''', Id (Identifier n.Uid), true
      | _ -> failwithf "Invalid parameters to when: %A" paramExps
  | _ -> let deps, g', paramExps' = List.fold_left (fun (depsAcc, g, exprs) expr ->
                                                      let deps, g', expr', _ = dataflowE env g expr
                                                      Set.union depsAcc deps, g', exprs @ [expr'])
                                                   (Set.empty, graph, List.empty) paramExps
         deps, g', FuncCall (fn, paramExps'), true

and makeSubExprBuilder (arg, argType, argMaker) expr deps =
  let mergeMaps a b = Map.fold_left (fun acc k v -> Map.add k v acc) a b

  let argInfo = { Uid = arg; Type = argType; MakeOper = argMaker; Name = arg; ParentUids = [] }
 
  // Create an initial environment and graph based on the list of dependencies found earlier
  let env, graph = Set.fold_left (fun (env, graph) n -> Map.add n.Uid n env,
                                                        Graph.add ([], n.Uid, n, []) graph)
                                 (Map.add arg argInfo Map.empty,
                                  Graph.add ([], arg, argInfo, []) Graph.empty)
                                 deps
  let roots = env |> Map.to_list |> List.map fst
              
  let deps', g', expr', _ = dataflowE env graph expr

  // deps' contains "new" dependencies that must be added to the environment
  // in case dataflowE gets called again on expr'
  let env' = Set.fold_left (fun acc info -> Map.add info.Uid info acc) env deps'

  (fun prio operators -> let fixPrio p = Priority.add (Priority.down prio) p
                         let operators' = makeOperNetwork g' roots fixPrio operators
                         let final = makeFinalOper env' g' expr' deps' operators' (Priority.down prio) arg
                         operators'.[arg], final)

(* Create the necessary nodes to evaluate an expression.
 *  - If the expression is a simple variable access, no new nodes are necessary;
 *  - If the expression is a record, we will need new nodes for each field and
 *    another node for the entire record;
 *  - If the expression is of any other kind, we create a general purpose
 *    evaluator node.
 *)
and makeFinalNode env graph expr deps name =
  match expr with
  (* No additional node necessary *)
  | Id (Identifier uid) -> Set.choose deps, graph
 
  (* Yes, the result is a record and we need the corresponding operator *)
  | Record fields ->
      (* Extend the environment to include dependencies of all the subexpressions *)
      let env' = Set.fold_left (fun acc info -> Map.add info.Uid info acc) env deps
      let g'', fieldDeps = List.fold_left (fun (g, fieldDeps) (Symbol field, expr) ->
                                             (* Lets get the dependencies for just this expression *)
                                             let deps'', g', expr', isDone = dataflowE env' g expr
                                            
                                             (* Sanity check: at this point, the expression should be completely
                                                "continualized" and thus, the previous operation must not have
                                                altered the graph nor the expression itself *)
                                             assert (g' = g && expr' = expr && isDone)
                                             
                                             (* Do we need an evaluator just for this field? *)
                                             let n, g' = makeFinalNode env' g expr deps'' field
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
  | _ -> createNode (nextSymbol name) (typeOf (envTypes env) expr) (Set.to_list deps) (makeEvaluator expr) graph
         
(* Similar to makeFinalNode, but creates operators instead of graph nodes *)
and makeFinalOper env graph expr deps (operators:Map<string, Operator>) (startPrio:Priority.priority) name =
   match expr with
   (* No additional node necessary *)  
   | Id (Identifier uid) -> operators.[uid]

   (* Yes, the result is a record and we need the corresponding operator *)
   | Record fields ->
       // Handle priorities with care
       let lastPrio, fieldDeps =
         List.fold_left (fun (prio, acc) (Symbol field, expr) ->
                           (* Lets get the dependencies for just this expression *)
                           let deps', g', expr', _ = dataflowE env graph expr
                           (* Sanity check: at this point, the expression should be completely
                              "continualized" and thus, the previous operation must not have
                              altered the graph nor the expression itself *)
                           assert (g' = graph && expr' = expr)
                           let op = makeFinalOper env graph expr deps' operators (Priority.down prio) field
                           (prio, acc @ [(field, op)]))
                        (startPrio, []) fields 
       let oper = makeRecord fieldDeps (nextSymbol ("finalRecord-" + name))
                             (Priority.next lastPrio) (List.map snd fieldDeps)
       //printfn "Created operator %s with priority %A" oper.Uid oper.Priority
       oper                             
                
   (* The result is an arbitrary expression and we need to evaluate it *)
   | _ -> let resultParents = Set.fold_left (fun acc v -> operators.[v.Uid]::acc) [] deps
          let oper = makeEvaluator expr (nextSymbol ("groupResult-" + name)) (Priority.next startPrio)
                                   resultParents
          //printfn "Created operator %s with priority %A" oper.Uid oper.Priority
          oper

and dataflowAggregate graph target paramExprs opMaker aggrName =
  let getField = match paramExprs, target.Type with
                 | [SymbolExpr (Symbol name)], TyStream _ -> (fun (VEvent ev) -> ev.[name])
                 | [SymbolExpr (Symbol name)], TyWindow (TyStream _, TimedWindow _) -> (fun (VEvent ev) -> ev.[name])
                 | [], TyInt -> id
                 | [], TyWindow (TyInt, TimedWindow _) -> id
                 | _ -> failwith "Invalid parameters to %s" aggrName
  let n, g' = createNode (nextSymbol aggrName) TyInt [target]
                         (opMaker getField) graph
  Set.singleton n, g', Id (Identifier n.Uid)

and dataflowGroupby env graph target paramExps =
  let field, expr, env', arg =
    match paramExps with
    | [SymbolExpr (Symbol field); Lambda ([Identifier arg], body) as fn] ->
        field, body,
        // Put the argument as an Unknown node into the environment
        // This way it will be ignored by the dataflow algorithm
        Map.add arg (NodeInfo.AsUnknown(arg)) env,
        arg
    | _ -> failwith "Invalid parameter to groupby"
  let argType = target.Type
  let argMaker = match argType with
                 | TyStream _ -> makeStream
                 | TyWindow (TyStream _, TimedWindow _) -> makeSimpleWindow
                 | _ -> failwith "Can't happen! But will"
  
  let deps, g', expr', _ = dataflowE env' graph expr
  let expr'' = Lambda ([Identifier arg], expr')
  let groupBuilder = makeSubExprBuilder (arg, argType, argMaker) expr' deps
  // GroupBy depends on the stream and on the dependencies of the predicate.
  let groupbyDeps = target::(Set.to_list deps)
  let valueType = typeOf ((envTypes env).Add(arg, argType)) expr
  let n, g'' = createNode (nextSymbol "groupby") (TyDict valueType) groupbyDeps
                         (makeGroupby field groupBuilder) g'
  Set.singleton n, g'', Id (Identifier n.Uid)

(*
 * Handles stream.where() and stream.select()
 *)
and dataflowSelectWhere env graph target paramExps opMaker methName =
  let subExpr, env', arg =
    match paramExps with
    | [Lambda ([Identifier arg], body) as fn] ->
        body,
        // Put the argument as an Unknown node into the environment
        // This way it will be ignored by the dataflow algorithm
        Map.add arg (NodeInfo.AsUnknown(arg)) env,
        arg
    | _ -> failwith "Invalid parameter to where"
  let deps, g', expr', _ = dataflowE env' graph subExpr
  let expr'' = Lambda ([Identifier arg], expr')
 
  // Depends on the stream and on the dependencies of the predicate.
  let opDeps = target::(Set.to_list deps)
  let n, g'' = createNode (nextSymbol methName) target.Type opDeps
                          (opMaker expr'') g'
  Set.singleton n, g'', Id (Identifier n.Uid)


(*
 * Iterate the graph and create the operators and the connections between them.
 *)
and makeOperNetwork (graph:DataflowGraph) (roots:string list) fixPrio operators : Map<string, Operator> =
  let order = Graph.Algorithms.topSort roots graph

  // Maps uids to priorities
  let orderPrio = List.fold_left (fun (acc, prio) x -> (Map.add x prio acc, Priority.next prio))
                                 (Map.empty, Priority.initial) order |> fst

  // Fold the graph with the right order, returning a map of all the operators
  let operators' =
    Graph.foldSeq (fun operators (pred, uid, info, succ) ->
                     // If the operator already exists, skip it.
                     if Map.mem uid operators
                       then operators
                       else let parents = List.map (fun uid -> Map.find uid operators) info.ParentUids
                            let op = info.MakeOper uid (fixPrio orderPrio.[uid]) parents
                            //printfn "Created operator %s with priority %A" uid (fixPrio orderPrio.[uid])
                            Map.add uid op operators)
                  operators graph order

  operators'

