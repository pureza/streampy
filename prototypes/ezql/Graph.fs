#light

type 'a Node = 'a
type 'a Adj = Node<'a> list
type Context<'a, 'b> = Adj<'a> * Node<'a> * 'b * Adj<'a>

module InternalTypes =

    type 'a Adj' = Set<'a Node>
    type Context'<'a, 'b> = Adj'<'a> * 'b * Adj'<'a>
    type GraphRep<'a, 'b> = Map<'a Node, Context'<'a, 'b>>


module private Util =

    open InternalTypes

    let (|EmptySet|ConsSet|) (s:Set<'a>) =
      if Set.is_empty s
        then EmptySet
        else let any = Set.choose s
             let s' = Set.remove any s
             ConsSet (any, s')

    let (|ExtractPair|_|) key (m:Map<'k, 'v>) =
      match Map.tryfind key m with
      | None -> None
      | Some v -> Some ((key, v), Map.remove key m)

    let (|ExtractAnyPair|_|) (m:Map<'k, 'v>) =
      match Map.first (fun k v -> Some (k, v)) m with
      | None -> None
      | Some (k, v) -> Some ((k, v), Map.remove k m)

    let addSucc v (p, l', s) : Context'<'a, 'b> = (p, l', Set.add v s)
    let addPred v (p, l', s) : Context'<'a, 'b> = (Set.add v p, l', s)

    let clearSucc v (p, l, s) : Context'<'a, 'b> = (p, l, Set.remove v s)
    let clearPred v (p, l, s) : Context'<'a, 'b> = (Set.remove v p, l, s)

    let rec updAdj (adj:Adj'<'a>) fn (gr:GraphRep<'a, 'b>) =
      match adj with
      | EmptySet -> gr
      | ConsSet (v, s') ->
          if Map.mem v gr
            then updAdj s' fn (Map.add v (fn gr.[v]) gr)
            else failwithf "Map doesn't contain %A" v


open InternalTypes
open Util

type Graph<'a, 'b>(contents:GraphRep<'a, 'b>) =
    member self.IsEmpty = contents.IsEmpty

    member self.Add((p, v, l, s)) =
      let p' = Set.of_list p
      let s' = Set.of_list s
      let g' = contents.Add(v, (p', l, s')) |> updAdj p' (addSucc v) |> updAdj s' (addPred v)
      Graph g'

    member self.Remove(v) =
      match contents with
      | ExtractPair v ((_, (p, l, s)), g') ->
          let p' = Set.filter ((<>) v) p
          let s' = Set.filter ((<>) v) s
          let gr = g' |> updAdj s' (clearPred v) |> updAdj p' (clearSucc v)
          Graph gr
      | _ -> self

    member self.Extract(v) : Option<Context<'a, 'b>> * Graph<'a, 'b> =
      match contents with
      | ExtractPair v ((_, (p, l, s)), g') -> Some (Set.to_list p, v, l, Set.to_list s), self.Remove(v)
      | _ -> (None, self)

    member self.ExtractAny () : Option<Context<'a, 'b>> * Graph<'a, 'b> =
      match contents with
      | ExtractAnyPair ((v, (p, l, s)), g') -> Some (Set.to_list p, v, l, Set.to_list s), self.Remove(v)
      | _ -> (None, self)

    member self.Item with get(v) = match self.Extract(v) |> fst with
                                   | Some ctx -> ctx
                                   | _ -> failwithf "Node doesn't exist: %A" v
    member self.LabelOf(v) = let _, _, l, _ = self.[v]
                             l

    override self.ToString() = Map.fold_left (fun acc k v -> acc + (sprintf "%A" v)) "" contents
    static member Empty () = Graph(Map.empty)


let (|Extract|_|) v (graph:Graph<'a, 'b>) =
  match graph.Extract(v) with
  | (None, _) -> None
  | (Some ctx, g') -> Some (ctx, g')


let (|ExtractAny|_|) (graph:Graph<'a, 'b>) =
  match graph.ExtractAny() with
  | (None, _) -> None
  | (Some ctx, g') -> Some (ctx, g')


[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Graph =
  let empty<'a, 'b> : Graph<'a, 'b> = Graph<'a, 'b>.Empty()
  let is_empty (gr:Graph<'a, 'b>) : bool = gr.IsEmpty
  let add (ctx:Context<'a, 'b>) (gr:Graph<'a, 'b>) : Graph<'a, 'b> = gr.Add(ctx)
  let mem v g = match g with
                | Extract v (_, _) -> true
                | _ -> false
  let extract (v:Node<'a>) (gr:Graph<'a, 'b>) : Option<Context<'a, 'b>> * Graph<'a, 'b> = gr.Extract(v)
  let extractAny (gr:Graph<'a, 'b>) : Option<Context<'a, 'b>> * Graph<'a, 'b> = gr.ExtractAny()

  let labelOf (v:Node<'a>) (graph:Graph<'a, 'b>) = graph.LabelOf(v)
  let suc v g = match g with
                | Extract v ((_, _, _, s), _) -> s
                | _ -> failwithf "Node doesn't exist."

  let rec fold (fn:'state -> Context<'a, 'b> -> 'state) (initial:'state) (graph:Graph<'a, 'b>) : 'state =
    match graph with
    | ExtractAny (ctx, gr) -> fn (fold fn initial gr) ctx
    | _ -> initial

  (* Fold with a given order *)
  let rec foldSeq (fn:'state -> Context<'a, 'b> -> 'state) (acc:'state)
                  (graph:Graph<'a, 'b>) (sequence:Node<'a> list) : 'state =
    match sequence with
    | x::xs -> match graph with
               | Extract x (ctx, gr) -> foldSeq fn (fn acc ctx) gr xs
               | _ -> failwith "Graph is smaller than the sequence!"
    | _ -> acc

  let map (fn:Context<'a, 'b> -> Context<'a, 'c>) = fold (fun acc ctx -> add (fn ctx) acc) empty
  let remove v graph = snd (extract v graph)
  let nodes graph = fold (fun acc (_, v, _, _) -> v::acc) [] graph

  module Algorithms =
    let rec dfs = function
      | [], g -> []
      | v::vs, g -> match g with
                    | Extract v (ctx, g') -> v::(dfs ((suc v g)@vs, g'))
                    | _ -> dfs (vs, g)


    let topSort roots graph =
        let rec dfsPostOrder = function
          | [], g -> ([], g)
          | v::vs, g -> match g with
                        | Extract v (ctx, g') -> let v1, g1 = dfsPostOrder ((suc v g), g')
                                                 let v2, g2 = dfsPostOrder (vs, g1)
                                                 v1@(v::v2), g2
                        | _ -> dfsPostOrder (vs, g)

        dfsPostOrder(roots, graph) |> fst |> List.rev


  module Viewer =

    open System.Windows.Forms
    open System.Drawing

    let display graph pp =

      let toGleeGraph graph =
        fold (fun (acc:Microsoft.Glee.Drawing.Graph) (p, v, l, s) ->
                for node in p do
                  let nodeL = (labelOf node graph)
                  acc.AddEdge(pp node nodeL, pp v l) |> ignore

                acc.AddNode (pp v l) |> ignore

                for node in s do
                  let nodeL = (labelOf node graph)
                  acc.AddEdge(pp v l, pp node nodeL) |> ignore

                acc)
             (new Microsoft.Glee.Drawing.Graph ("graph")) graph

      let createWindow () =
        let form = new Form(Text="Graph viewer", Size=new Size(800, 600))
        let gViewer = new Microsoft.Glee.GraphViewerGdi.GViewer(Dock=DockStyle.Fill)
        form.Controls.Add(gViewer)
        form, gViewer

      let gleeGraph = toGleeGraph graph
      let form, gViewer = createWindow ()
      gViewer.Graph <- gleeGraph
      form.Show()
      Application.Run(form)


