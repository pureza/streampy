#light

open System
open System.Text.RegularExpressions
open Ast
open Types
open Graph
open Oper
open Eval

type NodeType =
  | Stream
  | DynVal
  | Dict
  | Unknown
  
type uid = string
type NodeInfo = { Uid:uid
                  Type:NodeType
                  mutable Name:string
                  MakeOper:uid -> priority -> oper }
 
                  interface IComparable with 
                    member self.CompareTo(other) =
                      match other with
                      | :? NodeInfo as other' -> String.compare self.Uid other'.Uid
                      | _ -> failwith "Go away. Other is not even a NodeInfo."
                      
                  override self.Equals(other) =
                    match other with
                      | :? NodeInfo as other' -> self.Uid = other'.Uid
                      | _ -> false
                      
                  static member DefaultWith(uid, typ, ?name, ?makeOper) =
                    let name' = match name with
                                | Some n -> n
                                | _ -> uid
                    let makeOper' = match makeOper with
                                    | Some m -> m
                                    | _ -> (fun _ _ -> failwith "Won't happen!")
                    { Uid = uid; Type = typ; Name = name'; MakeOper = makeOper' }

type DataflowGraph = Graph<string, NodeInfo>

let nextSymbol =
  let counter = ref 0
  (fun prefix -> counter := !counter + 1
                 prefix + (!counter).ToString())

let uid2name uid = Regex.Match(uid, "(.*)\d").Groups.[1].Value

let createNode uid typ pred makeOp graph =
  let node = { Uid = uid; Type = typ; Name = uid2name uid; MakeOper = makeOp }
  node, Graph.add (pred, uid, node, []) graph

type context = Map<string, NodeInfo>

let rec dataflow (env:context, (graph:DataflowGraph), roots) = function
  | Assign (Identifier name, exp) ->
      let deps, g', expr' = dataflowE env graph exp
      let roots' = name::roots
      let n, g' = match Set.to_list deps, expr' with
                  | [dep], Id (Identifier uid) -> dep, g'
                  | x::xs, _ -> let uids = Set.map (fun n -> n.Uid) deps |> Set.to_list
                                createNode (nextSymbol name) DynVal uids (fun uid prio -> arithm uid prio expr') graph
                  | [], _ -> failwith "constant node? Not supported (yet)"
      env.Add(name, Graph.labelOf n.Uid g'), g', roots'


and dataflowE (env:context) (graph:DataflowGraph) expr =
  match expr with
  | MethodCall (expr, (Identifier name), paramExps) ->
      let deps, g', expr' = dataflowE env graph expr
      match Set.to_list deps with
      | [dep] -> dataflowMethod env g' dep name paramExps
      | [] -> Set.empty, g', expr'
      | _ -> failwith "The target of the method call depends on more than one value"
  | ArrayIndex (expr, index) ->
      let deps, g', expr' = dataflowE env graph expr
      match Set.to_list deps with
      | [dep] -> dataflowMethod env g' dep "[]" [index]
      | [] -> Set.empty, g', expr'
      | _ -> failwith "The target of the window depends on more than one value"
  | FuncCall (Id (Identifier "stream"), paramExps) ->
      let n, g' = createNode (nextSymbol "stream") Stream [] (fun uid prio -> stream uid prio) graph
      Set.singleton n, g', Id (Identifier n.Uid)
  | MemberAccess (expr, (Identifier name)) ->
      let deps, g', expr' = dataflowE env graph expr
      match Set.to_list deps with
      | [dep] -> failwith "Ver isto"//deps, g', expr'
      | [] -> Set.empty, g', MemberAccess (expr', (Identifier name))
      | _ -> failwith "The target of the method call depends on more than one value"
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
    
and dataflowMethod env graph (target:NodeInfo) methName paramExps =
  match target.Type with
  | Stream -> match methName with
              | "last" -> let field = match paramExps with
                                      | [SymbolExpr (Symbol name)] -> name
                                      | _ -> failwith "Invalid parameters to last"
                          let n, g' = createNode (nextSymbol methName) DynVal [target.Uid] (fun uid prio -> last uid prio field) graph
                          Set.singleton n, g', Id (Identifier n.Uid)
              | "[]" -> let n, g' = createNode (nextSymbol "[x min]") Stream [target.Uid] (fun prio -> failwith "Windows are not supported. Do your homework.") graph
                        Set.singleton n, g', Id (Identifier n.Uid)
              | "where" -> let pred, env', arg = 
                             match paramExps with
                             | [Lambda ([Identifier arg], body) as fn] -> 
                                 body,
                                 // Put the argument as an Unknown node into the environment
                                 // This way it will be ignored by the dataflow algorithm
                                 Map.add arg (NodeInfo.DefaultWith(arg, Unknown)) env,
                                 arg
                             | _ -> failwith "Invalid parameter to where"
                           let deps, g', expr' = dataflowE env' graph pred
                           
                           // Turn the predicate into a function call that, when evaluated
                           // for a given event, returns true or false
                           let expr'' = FuncCall (Lambda ([Identifier arg], expr'), [Id (Identifier arg)])
                           
                           // Where depends on the stream and on the dependencies of the predicate.
                           let whereDeps = target.Uid::(List.map (fun n -> n.Uid) (Set.to_list deps))
                           let n, g'' = createNode (nextSymbol methName) Stream whereDeps (fun uid prio -> where uid prio expr'') g'
                           Set.singleton n, g'', Id (Identifier n.Uid)
              | _ -> failwithf "Unkown method: %s" methName
  | _ -> failwith "Unknown target type"

//let rec dataflow (env:Map<string, uid>, roots, (graph:DataflowGraph)) = function
//  | Assign (Identifier name, exp) ->
//      let node, g' = dataflowE env graph exp
//      match node with
//      | Some node' -> node'.Name <- sprintf "%s (%s)" name (uid2name node'.Uid)
//                      let roots' = name::roots //match node'.Type with
//                                   //| Stream -> name::roots
//                                   //| _ -> roots
//                      env.Add(name, node'.Uid), roots', g'
//      | _ -> env, roots, g'
//      (*
//      // If this expression has no dependencies, it is a constant and we
//             // should be able to evaluate it.
//             let n, g' = createNode (nextSymbol "const") DynVal [] (fun uid prio -> constant uid prio exp) graph
//             n.Name <- sprintf "%s (%s)" name (uid2name n.Uid)
//             let roots' = name::roots
//             env.Add(name, n.Uid), roots', g'
//*)
//
//
//and dataflowE env graph = function
//  | MethodCall (expr, (Identifier name), paramExps) ->
//      let target, g' = dataflowE env graph expr
//      match target with
//      | Some target' -> dataflowMethod target' name paramExps g'
//      | _ -> None, g'
//  | FuncCall (Id (Identifier "stream"), paramExps) ->
//      let n, g' = createNode (nextSymbol "stream") Stream [] (fun uid prio -> stream uid prio) graph
//      Some n, g'      
//  | ArrayIndex (expr, index) ->
//      let target, g' = dataflowE env graph expr
//      match target with
//      | Some target' -> let n, g'' = createNode (nextSymbol "[x min]") DynVal [target'.Uid] (fun prio -> failwith "Windows are not supported. Do your homework.") g'
//                        Some n, g''
//      | _ -> None, g'
//  | BinaryExpr (oper, expr1, expr2) as expr ->
//      let g', allDeps, expr' = dataflowBinExp oper expr1 expr2 env graph
//      let some = [ for t in allDeps -> t.Uid ]
//      let n, g'' = createNode (nextSymbol (sprintf "%A" oper)) DynVal some (fun uid prio -> arithm uid prio expr') g'
//      Some n, g''
//  | Integer i -> None, graph
//  | Id (Identifier name) -> 
//      let nodeInfo = Option.bind (fun v -> graph.[v]) (Map.tryfind name env)
//      match nodeInfo with
//      | Some (_, _, info, _) -> Some info, graph
//      | _ -> failwithf "Identifier not found in the graph: %s" name
//  | _ -> failwith "Expression not recognized"      
//        
//and dataflowMethod (target:NodeInfo) name paramExps graph =
//  match target.Type with
//  | Stream -> match name with
//              | "last" -> let field = match paramExps with
//                                      | [SymbolExpr (Symbol name)] -> name
//                                      | _ -> failwith "Invalid parameters to last"
//                          let n, g' = createNode (nextSymbol name) DynVal [target.Uid] (fun uid prio -> last uid prio field) graph
//                          Some n, g'
//              | "where" -> let pred = match paramExps with
//                                      | [Lambda (args, body) as fn] -> eval Map.empty fn
//                                      | _ -> failwith "Invalid parameters to where"
//                           let pred' = (fun ev -> match evalClosure pred [VEvent ev] with
//                                                  | VBool b -> b
//                                                  | _ -> failwith "Where predicate is supposed to return a boolean!")
//                           let n, g' = createNode (nextSymbol name) Stream [target.Uid] (fun uid prio -> where uid prio pred') graph
//                           Some n, g'
//              | "groupby" -> let n, g' = createNode (nextSymbol name) Dict [target.Uid] (fun uid prio -> failwith "groupby not supported") graph
//                             Some n, g'
//              | _ -> failwithf "Unkown method: %s" name
//  | DynVal -> match name with
//              | "max" -> let n, g' = createNode (nextSymbol name) DynVal [target.Uid] (fun uid prio -> failwith "max not supported") graph
//                         Some n, g'
//              | _ -> failwithf "Unkown method: %s" name
//  | Dict -> match name with
//              | _ -> failwithf "Unkown method: %s" name
//
//(*
// * Here, we avoid creating one node per binary expression.
// * For example, for the expression (x + y * (z + x)), we create
// * a single node that depends on x, y and z and evaluates the entire
// * expression when any dependency is updated.
// *
// * This function analyzes the entire expression and collects its
// * dependencies. It also builds a new expression where the names of 
// * referenved variables are replaced by their corresponding node's uid.
// * This is important to handle more complex expressions. For example, in
// * (x + y[3 min].max()), a node n is created specifically for y[3 min].max()
// * and the resulting expression is (x.Uid + n.Uid).
// *)
//and dataflowBinExp op expr1 expr2 env graph : DataflowGraph * Set<NodeInfo> * expr =
//  let rec findDeps expr graph =
//    match expr with
//    | BinaryExpr (op, expr1, expr2) -> dataflowBinExp op expr1 expr2 env graph
//    | _ -> let n, g' = dataflowE env graph expr
//           match n with
//           | Some n' -> let expr' = Id (Identifier n'.Uid)
//                        (g', Set.singleton n', expr')
//           |_ -> (g', Set.empty, expr)
//  
//  let g1, deps1, expr1' = findDeps expr1 graph
//  let g2, deps2, expr2' = findDeps expr2 g1
//  g2, Set.union deps1 deps2, BinaryExpr (op, expr1', expr2')
//
//and evalClosure = function
//    | VClosure (env, expr) ->
//        match expr with
//        | Lambda (ids, body) ->
//            (fun args ->
//                let ids' = List.map (fun (Identifier name) -> name) ids
//                let env' = List.fold_left (fun e (n, v) -> Map.add n v e) 
//                                           env (List.zip ids' (List.map ref args))
//                eval env' body)
//        | _ -> failwith "evalClosure: Wrong type"
//    | _ -> failwith "This is not a closure"
//

(*
 * Iterate the graph and create the operators and the connections between them.
 *)
let makeOperNetwork (graph:DataflowGraph) (roots:string list) : Map<string, oper> =      
  let order = Graph.Algorithms.topSort roots graph
  
  // Maps uids to priorities
  let orderPrio = List.fold_left (fun (acc, prio) x -> (Map.add x prio acc, prio + 1.0)) 
                                 (Map.empty, 0.0) order |> fst

  // Fold the graph, returning:
  // - Map of operators
  // - Continuation that receives the map and created the necessary connections.  
  let operators, connections = 
    Graph.fold (fun acc (pred, uid, info, succ) -> 
                  let operators, connections = acc
                  let op = info.MakeOper uid orderPrio.[uid]
                  (Map.add uid op operators, connections
                                             @ List.mapi (fun i p -> (Map.find p operators, op, i)) pred
                                             @ List.mapi (fun i s -> (op, Map.find s operators, pred.Length + i)) succ)) 
               (Map.empty, []) graph

  // Sort the list of connections by index
  let connections' = List.sort (fun (_, _, i1) (_, _, i2) -> Int32.compare i1 i2) connections
  
  // Now create the connections
  for src, dst, index in connections' do
    connect src dst id

  operators

