﻿open Ast
open Types
open TypeChecker
open Extensions

type PrimitiveFunction =
  { Name:string
    TypeCheck:PrimTyChecker
    Eval:PrimEvaluator }


let print =
  { Name = "print"
    TypeCheck = fun env param ->
                  typeOf env param |> ignore
                  TyUnit, []
    Eval = fun eval env param ->
             let v = eval env param
             printfn "%O" v
             VUnit }


let makeEnum =
  { Name = "$makeEnum"
    TypeCheck = fun env param ->
                  match param with
                  | Tuple [Id (Identifier label); meta] ->
                      let tm, cm = constr env meta
                      let tr = fresh ()
                      let tl = TyArrow (tm, tr)
                      tr, SameType (tl, getType env.[label])::cm
                  | _ -> failwithf "Invalid parameter to $makeEnum: %A" param
    Eval = fun eval env param ->
             match param with
             | (Tuple ([Id (Identifier label); meta])) -> VVariant (label, eval env meta)
             | _ -> failwithf "Invalid parameter to $makeEnum: %A" param }


let metadata =
  { Name = "$metadata"
    TypeCheck = fun env param ->
                  match param with
                  | Tuple ([String label; var]) when Map.contains label env ->
                      match getType (env.[label]) with
                      | TyArrow (ty, _) -> ty, []
                      | other -> failwithf "Can't happen. The type of %s must be a TyArrow, but is a %A" label other
                  | _ -> failwithf "Invalid parameter to $makeEnum: %A" param
    Eval = fun eval env param ->
             match eval env param with
             | VTuple [expec; VVariant (label, meta)] when VString label = expec -> meta
             | other -> failwithf  "$metadata: The variant is not a variant but %A" other }


let whenFun =
  { Name = "when"
    TypeCheck = fun env param ->
                  match param with
                  | Tuple [source; Lambda ([Param (Id (Identifier ev), _)], body) as handler] ->
                      let ts, cs = constr env source
                      match constr env handler with
                      | TyArrow (evTy, t2), ch ->
                          t2, (SameType (ts, TyStream evTy))::(cs @ ch)
                      | _ -> failwithf "Can't happen"
                  | _ -> failwithf "Invalid parameter to when: %A" param
    Eval = fun _ _ _ -> failwithf "Will never be called" }


let listenN =
  { Name = "listenN"
    TypeCheck = fun env param ->
                  match param with
                  | Tuple (initial::rest) ->
                      let ty, ci = constr env initial
                      let cs = List.fold (fun ccs listener ->
                                       let listym, cl = constr env listener
                                       SameType (listym, TyArrow (ty, ty))::(cl @ ccs))
                                    ci rest
                      ty, cs
                  | _ -> failwithf "Invalid parameter to when: %A" param
    Eval = fun _ _ _ -> failwithf "Will never be called" }


let merge =
  { Name = "merge"
    TypeCheck = fun env param ->
                  match param with
                  | Tuple ([stream1; stream2; SymbolExpr (Symbol field)]) ->
                      match typeOf env stream1, typeOf env stream2 with
                      | TyStream (TyRecord fields1), TyStream (TyRecord fields2) ->
                          if Map.contains field fields1 && Map.contains field fields2
                            then TyStream (TyRecord (Map.merge (fun a b -> a) fields1 fields2)), []
                            else failwithf "Both streams must contain field %A" field
                      | _ -> failwithf "Invalid parameter to when: %A" param
                  | _ -> failwithf "Invalid parameter to when: %A" param
    Eval = fun _ _ _ -> failwithf "Will never be called" }


let primitiveFunctions = [print; makeEnum; metadata; whenFun; listenN; merge]