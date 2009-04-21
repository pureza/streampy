#light

open System
open System.Text.RegularExpressions
open Ast
open Types
open Graph
open Oper
open CommonOpers
open AggregateOpers
open DictOpers
open Eval

type NodeType =
  | Stream
  | Window
  | DynVal
  | Dict
  | Unknown

type uid = string
type NodeInfo =
  { Uid:uid
    Type:NodeType
    mutable Name:string
    // uid -> priority -> parents -> created operator
    MakeOper:uid -> priority -> Operator list -> Operator
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

      { Uid = uid; Type = Unknown; Name = uid; MakeOper = makeOper';
        ParentUids = [] }

    static member UidOf(nodeInfo) = nodeInfo.Uid

type DataflowGraph = Graph<string, NodeInfo>

let nextSymbol =
  let counter = ref 0
  (fun prefix -> counter := !counter + 1
                 prefix + (!counter).ToString())

let uid2name uid = Regex.Match(uid, "(.*)\d").Groups.[1].Value

let createNode uid typ parentUids makeOp graph =
  let node = { Uid = uid; Type = typ; Name = uid2name uid;
               MakeOper = makeOp; ParentUids = parentUids }
  node, Graph.add (parentUids, uid, node, []) graph

type context = Map<string, NodeInfo>

let rec dataflow (env:context, (graph:DataflowGraph), roots) = function
  | Assign (Identifier name, exp) ->
      let deps, g', expr' = dataflowE env graph exp
      let roots' = name::roots
      let n, g' = match Set.to_list deps, expr' with
                  | [dep], Id (Identifier uid) -> dep, g'
                  | x::xs, _ -> let uids = Set.map NodeInfo.UidOf deps |> Set.to_list
                                createNode (nextSymbol name) DynVal uids (makeEvaluator expr') g'
                  | [], _ -> failwith "constant node? Not supported (yet)"
      env.Add(name, Graph.labelOf n.Uid g'), g', roots'


and dataflowE (env:context) (graph:DataflowGraph) expr =
  match expr with
  | MethodCall (target, (Identifier name), paramExps) ->
      let deps, g', expr' = dataflowE env graph target
      match Set.to_list deps with
      | [dep] -> dataflowMethod env g' dep name paramExps
      | [] -> Set.empty, g', expr
      | _ -> failwith "The target of the method call depends on more than one value"
  | ArrayIndex (expr, index) ->
      let deps, g', expr' = dataflowE env graph expr
      match Set.to_list deps with
      | [dep] -> dataflowMethod env g' dep "[]" [index]
      | [] -> Set.empty, g', expr'
      | _ -> failwith "The target of the window depends on more than one value"
  | FuncCall (Id (Identifier "stream"), paramExps) ->
      let n, g' = createNode (nextSymbol "stream") Stream [] makeStream graph
      Set.singleton n, g', Id (Identifier n.Uid)
  | MemberAccess (expr, (Identifier name)) ->
      let deps, g', expr' = dataflowE env graph expr
      match Set.to_list deps with
      | [dep] -> failwith "Ver isto"//deps, g', expr'
      | [] -> Set.empty, g', MemberAccess (expr', (Identifier name))
      | _ -> failwith "The target of the method call depends on more than one value"
  | Record fields ->
      let deps, g', exprs =
        List.fold_left (fun (depsAcc, g, expsAcc) (Symbol field, expr) ->
                          let deps, g', expr' = dataflowE env g expr
                          match expr' with
                          | Id (Identifier uid) -> depsAcc @ (Set.to_list deps), g', expsAcc @ [(Symbol field, expr')]
                          | _ -> depsAcc @ (Set.to_list deps), g', expsAcc @ [(Symbol field, expr')])
                       ([], graph, []) fields
      Set.of_list deps, g', Record exprs
  | BinaryExpr (oper, expr1, expr2) as expr ->
      let deps1, g1, expr1' = dataflowE env graph expr1
      let deps2, g2, expr2' = dataflowE env g1 expr2
      Set.union deps1 deps2, g2, BinaryExpr (oper, expr1', expr2')
  | Id (Identifier name) ->
      let info = Map.tryfind name env
      match info with
      | Some info' -> if info'.Type <> Unknown
                        then Set.singleton info', graph, Id (Identifier info'.Uid)
                        else Set.empty, graph, expr // If the type is unknown, nothing depends on it.
      | _ -> failwithf "Identifier not found in the graph: %s" name
  | Integer i -> Set.empty, graph, expr
  | _ -> failwithf "Expression type not supported: %A" expr

and dataflowMethod env graph (target:NodeInfo) methName paramExps =
  match target.Type with
  | Stream -> match methName with
              | "last" -> dataflowAggregate graph target paramExps makeLast methName
              | "sum" -> dataflowAggregate graph target paramExps makeSum methName
              | "[]" -> let duration = match paramExps with
                                       | [Time (Integer v, unit) as t] -> toSeconds v unit
                                       | _ -> failwith "Invalid duration"
                        let n, g' = createNode (nextSymbol "[x min]") Window [target.Uid]
                                               (makeWindow duration) graph
                        Set.singleton n, g', Id (Identifier n.Uid)
              | "where" -> let pred, env', arg =
                             match paramExps with
                             | [Lambda ([Identifier arg], body) as fn] ->
                                 body,
                                 // Put the argument as an Unknown node into the environment
                                 // This way it will be ignored by the dataflow algorithm
                                 Map.add arg (NodeInfo.AsUnknown(arg)) env,
                                 arg
                             | _ -> failwith "Invalid parameter to where"
                           let deps, g', expr' = dataflowE env' graph pred
                           let expr'' = Lambda ([Identifier arg], expr')

                           // Where depends on the stream and on the dependencies of the predicate.
                           let whereDeps = target.Uid::(List.map (fun n -> n.Uid) (Set.to_list deps))
                           let n, g'' = createNode (nextSymbol methName) Stream whereDeps
                                                   (makeWhere expr'') g'
                           Set.singleton n, g'', Id (Identifier n.Uid)
              | "groupby" -> let field, expr, env', arg =
                               match paramExps with
                               | [SymbolExpr (Symbol field); Lambda ([Identifier arg], body) as fn] ->
                                   field, body,
                                   // Put the argument as an Unknown node into the environment
                                   // This way it will be ignored by the dataflow algorithm
                                   Map.add arg (NodeInfo.AsUnknown(arg)) env,
                                   arg
                               | _ -> failwith "Invalid parameter to groupby"
                             let deps, g', expr' = dataflowE env' graph expr
                             let expr'' = Lambda ([Identifier arg], expr')
                             let groupBuilder = makeSubExprBuilder (arg, Stream, makeStream) expr' deps
                             // GroupBy depends on the stream and on the dependencies of the predicate.
                             let groupbyDeps = target.Uid::(List.map NodeInfo.UidOf (Set.to_list deps))
                             let n, g'' = createNode (nextSymbol methName) Dict groupbyDeps
                                                     (makeGroupby field groupBuilder) g'
                             Set.singleton n, g'', Id (Identifier n.Uid)
              | _ -> failwithf "Unkown method: %s" methName
  | Window -> match methName with
              | "last" -> dataflowAggregate graph target paramExps makeLast methName
              | "sum" -> dataflowAggregate graph target paramExps makeSum methName
              | "groupby" -> dataflowGroupby env graph target paramExps
              | _ -> failwithf "Unkown method of type Window: %s" methName
  | Dict -> match methName with
            | "where" -> let expr, env', arg =
                           match paramExps with
                           | [Lambda ([Identifier arg], body) as fn] ->
                               body,
                               // Put the argument as an Unknown node into the environment
                               // This way it will be ignored by the dataflow algorithm
                               Map.add arg (NodeInfo.AsUnknown(arg)) env,
                               arg
                           | _ -> failwith "Invalid parameter to dict/where"
                         let deps, g', expr' = dataflowE env' graph expr
                         let expr'' = Lambda ([Identifier arg], expr')
                         let predBuilder = makeSubExprBuilder (arg, DynVal, makeDynVal) expr' deps
                         // Where depends on the parent dictionary and on the dependencies of the predicate.
                         let whereDeps = target.Uid::(List.map NodeInfo.UidOf (Set.to_list deps))
                         let n, g'' = createNode (nextSymbol methName) Dict whereDeps
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
                          let deps, g', expr' = dataflowE env' graph expr
                          let expr'' = Lambda ([Identifier arg], expr')
                          let projBuilder = makeSubExprBuilder (arg, DynVal, makeDynVal) expr' deps
                          // Select depends on the parent dictionary and on the dependencies of the predicate.
                          let selectDeps = target.Uid::(List.map NodeInfo.UidOf (Set.to_list deps))
                          let n, g'' = createNode (nextSymbol methName) Dict selectDeps
                                                  (makeDictSelect projBuilder) g'
                          Set.singleton n, g'', Id (Identifier n.Uid)
            | _ -> failwithf "Unkown method: %s" methName
  | _ -> failwith "Unknown target type"


and makeSubExprBuilder (arg, argType, argMaker) expr deps =
  let mergeMaps a b = Map.fold_left (fun acc k v -> Map.add k v acc) a b

  let argInfo = { Uid = arg; Type = argType; MakeOper = argMaker; Name = arg; ParentUids = [] }
  let graph = Graph.add ([], arg, argInfo, []) Graph.empty
  let env = Set.fold_left (fun acc n -> Map.add n.Uid n acc)
                          Map.empty deps
              |> Map.add arg argInfo
              
  // deps' contains "new" dependencies that must be added to the environment
  // in case dataflowE gets called again on expr'
  let deps', g', expr' = dataflowE env graph expr
  let env' = Set.fold_left (fun acc info -> Map.add info.Uid info acc) env deps'

  (fun prio rtEnv -> let fixPrio p = prio + (p + 1.0) * 0.01
                     let operators = mergeMaps (makeOperNetwork g' [arg] fixPrio) rtEnv
                     (* Do we need a final operator to evaluate the expression?
                        If we do, deps' contains all the dependencies necessary
                        and we use "operators" to lookup the corresponding operators. *)
                     let final =
                       match expr' with
                       (* No additional node necessary *)
                       | Id (Identifier uid) -> operators.[uid]

                       (* Yes, the result is a record and we need the corresponding operator *)
                       | Record fields ->
                           let fieldDeps = List.mapi
                                             (fun i (Symbol field, expr) ->
                                                (* Lets get the dependencies for just this expression *)
                                                let deps'', g'', expr' = dataflowE env' g' expr
                                                (* Sanity check: at this point, the expression should be completely
                                                   "continualized" and thus, the previous operation must not have
                                                   altered the graph nor the expression itself *)
                                                assert (g'' = g' && expr' = expr)

                                                (* Do we need an evaluator just for this field? *)
                                                match expr, Set.to_list deps'' with
                                                | Id (Identifier name), [dep] -> (field, operators.[dep.Uid])
                                                | Id (Identifier name), _ -> failwith "This cannot happen... but will :)"
                                                | _ -> let parents = List.map (fun v -> operators.[v.Uid]) (Set.to_list deps'')
                                                       let prio = fixPrio (float (i + operators.Count + 1))
                                                       let evalOper = makeEvaluator expr (nextSymbol field) prio parents
                                                       (field, evalOper))
                                             fields
                           makeRecord fieldDeps (nextSymbol ("groupRecordResult-" + arg))
                                      (fixPrio (float (fieldDeps.Length + operators.Count + 1))) (List.map snd fieldDeps)
                       (* The result is an arbitrary expression and we need to evaluate it *)
                       | _ -> let finalPrio = fixPrio (float operators.Count + 1.0)
                              let resultParents = Set.fold_left (fun acc v -> operators.[v.Uid]::acc) [] deps'
                              makeEvaluator expr' (nextSymbol ("groupResult-" + arg)) finalPrio
                                            resultParents

                     operators.[arg], final)

and dataflowAggregate graph target paramExprs opMaker aggrName =
  let field = match paramExprs with
              | [SymbolExpr (Symbol name)] -> name
              | _ -> failwith "Invalid parameters to last"
  let n, g' = createNode (nextSymbol aggrName) DynVal [target.Uid]
                         (opMaker field) graph
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
                 | Stream -> makeStream
                 | Window -> makeSimpleWindow
                 | _ -> failwith "Can't happen! But will"
  let deps, g', expr' = dataflowE env' graph expr
  let expr'' = Lambda ([Identifier arg], expr')
  let groupBuilder = makeSubExprBuilder (arg, argType, argMaker) expr' deps
  // GroupBy depends on the stream and on the dependencies of the predicate.
  let groupbyDeps = target.Uid::(List.map NodeInfo.UidOf (Set.to_list deps))
  let n, g'' = createNode (nextSymbol "groupby") Dict groupbyDeps
                         (makeGroupby field groupBuilder) g'
  Set.singleton n, g'', Id (Identifier n.Uid)


(*
 * Iterate the graph and create the operators and the connections between them.
 *)
and makeOperNetwork (graph:DataflowGraph) (roots:string list) fixPrio : Map<string, Operator> =
  let order = Graph.Algorithms.topSort roots graph

  // Maps uids to priorities
  let orderPrio = List.fold_left (fun (acc, prio) x -> (Map.add x prio acc, prio + 1.0))
                                 (Map.empty, 0.0) order |> fst

  // Fold the graph with the right order, returning a map of all the operators
  let operators =
    Graph.foldSeq (fun acc (pred, uid, info, succ) ->
                     let operators = acc
                     let parents = List.map (fun uid -> Map.find uid operators) info.ParentUids
                     let op = info.MakeOper uid (fixPrio orderPrio.[uid]) parents
                     Map.add uid op operators)
                  Map.empty graph order

  operators

