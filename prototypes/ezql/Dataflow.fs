#light

open Ast
open Graph
open System.Text.RegularExpressions

type context = Map<string, string>
type NodeType =
  | Stream
  | DynVal
  | Dict
  
type NodeInfo = { Uid:string
                  Type:NodeType
                  mutable Name:string }


let nextSymbol =
  let counter = ref 0
  (fun prefix -> counter := !counter + 1
                 prefix + (!counter).ToString())


let uid2name uid = Regex.Match(uid, "(.*)\d").Groups.[1].Value

let createNode uid typ pred graph =
  let node = { Uid = uid; Type = typ; Name = uid2name uid }
  node, Graph.add (pred, uid, node, []) graph

let rec dataflow (env:context) roots (graph:Graph<string, NodeInfo>) = function
  | Assign (Identifier name, exp) ->
      let node, g' = dataflowE env graph exp
      match node with
      | Some node' -> node'.Name <- sprintf "%s (%s)" name (uid2name node'.Uid)
                      let roots' = match node'.Type with
                                   | Stream -> node'.Uid::roots
                                   | _ -> roots
                      env.Add(name, node'.Uid), roots', g'
      | _ -> env, roots, g'


and dataflowE env graph = function
  | MethodCall (expr, (Identifier name), paramExps) ->
      let target, g' = dataflowE env graph expr
      match target with
      | Some target' -> dataflowMethod target' name paramExps g'
      | _ -> None, g'
  | FuncCall (Id (Identifier "stream"), paramExps) ->
      let n, g' = createNode (nextSymbol "stream") Stream [] graph
      Some n, g'      
  | ArrayIndex (expr, index) ->
      let target, g' = dataflowE env graph expr
      match target with
      | Some target' -> let n, g'' = createNode (nextSymbol "[x min]") DynVal [target'.Uid] g'
                        Some n, g''
      | _ -> None, g'
  | BinaryExpr (oper, expr1, expr2) as expr ->        
        let t1, g1 = dataflowE env graph expr1
        let t2, g2 = dataflowE env g1 expr2
        let some = [ for t in [t1; t2] do 
                       match t with
                       | Some t' -> yield t'.Uid
                       | None -> () ]
        if some.IsEmpty
          then None, graph
          else let n, g' = createNode (nextSymbol (sprintf "%A" oper)) DynVal some g2
               Some n, g'
  | Integer i -> None, graph
  | Id (Identifier name) -> 
      let nodeInfo = Option.bind (fun v -> graph.[v]) (Map.tryfind name env)
      match nodeInfo with
      | Some (_, _, info, _) -> Some info, graph
      | _ -> failwithf "Identifier not found in the graph: %s" name
  | _ -> failwith "Expression not recognized"      
        
and dataflowMethod (target:NodeInfo) name paramExps graph =
  match target.Type with
  | Stream -> match name with
              | "last" -> let n, g' = createNode (nextSymbol name) DynVal [target.Uid] graph
                          Some n, g'
              | "groupby" -> let n, g' = createNode (nextSymbol name) Dict [target.Uid] graph
                             Some n, g'
              | _ -> failwithf "Unkown method: %s" name
  | DynVal -> match name with
              | "max" -> let n, g' = createNode (nextSymbol name) DynVal [target.Uid] graph
                         Some n, g'
              | _ -> failwithf "Unkown method: %s" name
  | Dict -> match name with
              | _ -> failwithf "Unkown method: %s" name
        