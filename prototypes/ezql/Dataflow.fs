#light

open Ast
open Graph

type context = Map<string, string>
type NodeType =
  | Stream
  
type NodeInfo = { Uid:string
                  Type:NodeType }


let nextSymbol =
  let counter = ref 0
  (fun prefix -> counter := !counter + 1
                 prefix + (!counter).ToString())

let rec dataflow (graph:Graph<string, NodeInfo>) = function
  | Assign (Identifier name, exp) ->
      let node, expg = dataflowE graph exp
      let right = { Uid = name; Type = Stream }
      Graph.add ([node.Uid], name, right, []) expg


and dataflowE graph = function
  | MethodCall (expr, (Identifier name), paramExps) ->
      let target, _ = dataflowE graph expr
      dataflowMethod target name paramExps graph
  | FuncCall (Id (Identifier "stream"), paramExps) ->
      let stream = { Uid = (nextSymbol "stream"); Type = Stream }
      stream, Graph.add ([], stream.Uid, stream, []) graph
      //dataflowFunction target name paramExps graph
  | Id (Identifier name) -> match graph.[name] with
                            | Some (_, _, info, _) -> info, graph
                            | _ -> failwithf "Identifier not found in the graph: %s" name
  | _ -> failwith "Expression not recognized"      
        
and dataflowMethod (target:NodeInfo) name paramExps graph =
  match target.Type with
  | Stream -> match name with
              | "last" -> let last = { Uid = (nextSymbol "last"); Type = Stream }
                          last, Graph.add ([target.Uid], last.Uid, last, []) graph
              | _ -> failwithf "Unkown method: %s" name
        