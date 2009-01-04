#light

type prog = Prog of clss list

and clss  = Class of string * mthd list

and mthd  = Method of string * id list * expr list

and expr  =
  | Const of int
  | Id of id
  | Assign of string * expr

and id = Identifier of string

