open Ast
open TypeChecker
open Engine
open Rewrite
open Util
open Extensions


let compile code =
    let ast = parse code |> rewrite1
    typeCheck ast


let parseType tyStr =
    let lexbuf = Lexing.from_string tyStr
    try
        Parser.Type Lexer.token lexbuf
    with e ->
        let pos = lexbuf.EndPos
        failwithf "Error near line %d, character %d\n" (pos.Line + 1) pos.Column


let rec matchTypes s t subst =
  match s, t with
  | _ when s = t -> true, subst
  | TyArrow (s1, s2), TyArrow (t1, t2) ->
      let valid, subst' = matchTypes s1 t1 subst
      first ((&&) valid) (matchTypes s2 t2 subst')
  | TyGen k, TyGen m ->
      match Map.tryFind k subst with
      | Some m' when m' <> m -> false, Map.empty
      | _ -> true, Map.add k m subst
  | TyRecord fields1, TyRecord fields2 ->
      let labels1 = Map.keySet fields1
      let labels2 = Map.keySet fields2
      if labels1 = labels2
        then Map.fold_left (fun (v, subst) label ty ->
                             first ((&&) v) (matchTypes ty fields2.[label] subst))
                           (true, subst) fields1
        else false, Map.empty
  | TyTuple elts1, TyTuple elts2 ->
      if elts1.Length = elts2.Length
        then List.fold (fun (v, subst) (ty1, ty2) ->
                          first ((&&) v) (matchTypes ty1 ty2 subst))
                       (true, subst) (List.zip elts1 elts2)
        else false, Map.empty   
  | TyStream t1, TyStream t2 -> matchTypes t1 t2 subst
             
        
    

let typeTest name code asserts =
  let types = compile code
  for (var, tyStr) in asserts do
    let ty = parseType tyStr
    if not (fst (matchTypes (getType types.[var]) ty Map.empty))
      then printfn "In %s: the type of '%s' is %A but should be %A" name var (getType types.[var]) ty


let runTypeTests () =
  let code = "
  a = 4;;
  
  id = fun x -> x;;
  
  apply = fun f x -> f x;;
  
  makeRec = fun a b -> { a = a, b = b };;
  
  makeTuple = fun a b -> (a, b);;
  
  idapp = id 3;;
  
  b = let { a = a, b = b } = { a = 3, b = 7 } in a * b;;
  
  c = let x = fun y z -> z * y + 5
      in x 10 10;;

  fact = let fact = fun n ->
                       if n == 0
                         then 1
                         else n * fact (n - 1)
         in
           fact;;

  poly = fun a -> (id a, id \"blah\");;
  
  fst = fun x ->
          let (a, _) = x in a;;

  getA = fun r ->
           let { a = a, b = b } = r in a;;
  
  id2 = let id = fun x -> x         
        in (id 3, id \"blah\");;
        
  getA2 = let getA2 = fun (r:{ a:'a }) (x:'a) -> r.a
          in (getA2 { a = 3 } 3, getA2 { a = \"blah\" } \"ola\");;
          
  getA3 = let getA3 = fun (r:{ a:'a }) -> fun (x:'a) -> r.a
          in (getA3 { a = 3 } 3, getA3 { a = \"blah\" } \"ola\");;
          
  onEvent = fun s f -> when (s, fun ev -> print (f ev));;
  
  define sum s f =
    let r = 0 when ev in s -> r + (f ev)
    in r;;
  "

  typeTest "one" code ["a",         "int";
                       "id",        "'x -> 'x";
                       "apply",     "('x -> 'y) -> 'x -> 'y"
                       "makeRec",   "'x -> 'y -> { a:'x, b:'y }"
                       "makeTuple", "'x -> 'y -> ('x, 'y)"
                       "idapp",     "int"
                       "b",         "int"
                       "c",         "int"
                       "fact",      "int -> int"
                       "poly",      "'a -> ('a, string)"
                       "fst",       "('a, 'b) -> 'a"
                       "getA",      "{ a:'a, b:'b } -> 'a"
                       "getA2",     "(int, string)"
                       "onEvent",    "stream<'a> -> ('a -> 'b) -> unit"
                       "sum",        "stream<'a> -> ('a -> int) -> int"]
