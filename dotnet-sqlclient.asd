;;;; dotnet-sqlclient.asd

(asdf:defsystem #:dotnet-sqlclient
  :description "Simple wrapper for .NET sql libraries, using RDNZL."
  :author "Raymond Wiker <rwiker@gmail.com>"
  :license "BSD0"
  :serial t
  :depends-on (rdnzl)
  :components ((:file "package")
               (:file "dotnet-sqlclient")))

