;;;; dotnet-sqlclient.lisp

(in-package #:dotnet-sqlclient)

;;; "dotnet-sqlclient" goes here. Hacks and glory await!

(defvar *system-data-assembly-loaded* nil)

(unless *system-data-assembly-loaded*
  (rdnzl:import-assembly "System.Data")

  (rdnzl:define-rdnzl-call connection-open (:dotnet-name "Open")
                           ((connection "System.Data.IDbConnection")))
  
  (rdnzl:define-rdnzl-call connection-close (:dotnet-name "Close")
                           ((connection "System.Data.IDbConnection")))

  (rdnzl:define-rdnzl-call connection-create-command (:dotnet-name "CreateCommand")
                           ((connection "System.Data.IDbConnection")))

  (rdnzl:define-rdnzl-call command-command-text (:dotnet-name "CommandText" :member-kind :property)
                           ((command "System.Data.IDbCommand")))

  (rdnzl:define-rdnzl-call (setf command-command-text) (:dotnet-name "CommandText" :member-kind :property)
                           ((command "System.Data.IDbCommand")))  

  (rdnzl:define-rdnzl-call command-execute-reader (:dotnet-name "ExecuteReader")
                           ((command "System.Data.IDbCommand")))

  (rdnzl:define-rdnzl-call command-execute-scalar (:dotnet-name "ExecuteScalar")
                           ((command "System.Data.IDbCommand")))

  (rdnzl:define-rdnzl-call command-execute-non-query (:dotnet-name "ExecuteNonQuery")
                           ((command "System.Data.IDbCommand")))

  (rdnzl:define-rdnzl-call command-dispose (:dotnet-name "Dispose")
                           ((command "System.IDisposable")))
  
  (rdnzl:define-rdnzl-call data-reader-read (:dotnet-name "Read")
                           ((reader "System.Data.IDataReader")))
  
  (rdnzl:define-rdnzl-call data-reader-get-value (:dotnet-name "GetValue")
                           ((reader "System.Data.IDataRecord")
                            (column "System.Int32")))

  (rdnzl:define-rdnzl-call data-reader-close (:dotnet-name "Close")
                           ((reader "System.Data.IDataReader")))

  (rdnzl:define-rdnzl-call object-type-of (:dotnet-name "GetType")
                           ((object "System.Object")))

  (rdnzl:define-rdnzl-call type-name (:dotnet-name "Name" :member-kind :property)
                           ((type "System.Type")))

  (rdnzl:define-rdnzl-call type-namespace (:dotnet-name "Namespace" :member-kind :property)
                           ((type "System.Type")))

  (rdnzl:define-rdnzl-call type-fullname (:dotnet-name "FullName" :member-kind :property)
                           ((type "System.Type")))

  (rdnzl:define-rdnzl-call object-to-string (:dotnet-name "ToString")
                           ((object "System.Object")))

  (rdnzl:define-rdnzl-call data-reader-get-field-type (:dotnet-name "GetFieldType")
                           ((reader "System.Data.IDataRecord")
                            (column "System.Int32")))

  (rdnzl:define-rdnzl-call data-reader-is-dbnull (:dotnet-name "IsDBNull")
                           ((reader "System.Data.IDataRecord")
                            (column "System.Int32")))

  (rdnzl:define-rdnzl-call data-reader-get-date-time (:dotnet-name "GetDateTime")
                           ((reader "System.Data.IDataRecord")
                            (column "System.Int32")))

  (rdnzl:define-rdnzl-call data-reader-get-string (:dotnet-name "GetString")
                           ((reader "System.Data.IDataRecord")
                            (column "System.Int32")))

  (rdnzl:define-rdnzl-call data-reader-get-int32 (:dotnet-name "GetInt32")
                           ((reader "System.Data.IDataRecord")
                            (column "System.Int32")))

  (rdnzl:define-rdnzl-call data-reader-get-int64 (:dotnet-name "GetInt64")
                           ((reader "System.Data.IDataRecord")
                            (column "System.Int32")))

  (rdnzl:define-rdnzl-call data-reader-get-decimal (:dotnet-name "GetDecimal")
                           ((reader "System.Data.IDataRecord")
                            (column "System.Int32")))

  (rdnzl:define-rdnzl-call data-reader-get-float (:dotnet-name "GetFloat")
                           ((reader "System.Data.IDataRecord")
                            (column "System.Int32")))

  (rdnzl:define-rdnzl-call data-reader-get-double (:dotnet-name "GetDouble")
                           ((reader "System.Data.IDataRecord")
                            (column "System.Int32")))

  (rdnzl:define-rdnzl-call data-reader-get-other (:dotnet-name "GetValue")
                           ((reader "System.Data.IDataRecord")
                            (column "System.Int32")))

  (rdnzl:define-rdnzl-call system-convert-to-base64-string (:dotnet-name "ToBase64String" :type-name "System.Convert")
                           ((bytes "System.Byte[]")))

  (rdnzl:define-rdnzl-call system-decimal-round (:dotnet-name "Round" :type-name "System.Decimal")
                           ((decimal "System.Decimal")))

  (rdnzl:define-rdnzl-call system-decimal-equals (:dotnet-name "Equals" :type-name "System.Decimal")
                           ((decimal-1 "System.Decimal")
                            (decimal-2 "System.Decimal")))

  (rdnzl:define-rdnzl-call system-decimal-to-int64 (:dotnet-name "ToInt64" :type-name "System.Decimal")
                           ((decimal "System.Decimal")))

  (rdnzl:define-rdnzl-call system-decimal-to-double (:dotnet-name "ToInt64" :type-name "System.Decimal")
                           ((decimal "System.Decimal")))

  (rdnzl:define-rdnzl-call system-datetime-to-string (:dotnet-name "ToString")
                           ((datetime "System.DateTime")
                            (format "System.String")))
  
  (setq *system-data-assembly-loaded* t))

(defun cast-system-decimal (obj)
  (if (system-decimal-equals obj (system-decimal-round obj))
    (system-decimal-to-int64 obj)
    (system-decimal-to-double obj)))

(defun system-datetime-as-iso (obj)
  (system-datetime-to-string obj "s"))

(defvar *oracle-assembly-loaded*
  nil)

(defun maybe-load-oracle-assembly ()
  (unless *oracle-assembly-loaded*
    (rdnzl:import-assembly "Oracle.DataAccess")
    (setq *oracle-assembly-loaded* t)))

(defun make-connection-string (data-source user-id password initial-catalog)
  (with-output-to-string (s)
    (format s "Data Source=~a;" data-source)
    (if (and (stringp user-id) (stringp password)
             (plusp (length user-id)) (plusp (length password)))
      (format s "User Id=~a;Password=~a;" user-id password)
      (format s "Integrated Security=SSPI;" data-source))
    (when initial-catalog
      (format s "Initial Catalog=~a;" initial-catalog))
    s))

(defun open-connection (data-source user-id password &key initial-catalog connection-string database-type)
  (let ((connection-string (or connection-string
                               (make-connection-string data-source user-id password initial-catalog))))
    (cond ((string= (symbol-name database-type) (symbol-name :oracle))
           (maybe-load-oracle-assembly)
           (let ((connection
                  (rdnzl:new "Oracle.DataAccess.Client.OracleConnection" connection-string)))
             (connection-open connection)
             connection))
          ((string= (symbol-name database-type) (symbol-name :sqlserver))
           (let ((connection
                  (rdnzl:new "System.Data.SqlClient.SqlConnection" connection-string)))
             (connection-open connection)
             connection))             
          (t
           (error "Unknown database type ~a" database-type)))))

(defun close-connection (connection)
  (rdnzl:invoke connection "Close"))

(defmacro with-open-connection ((connection
                                 (data-source user-id password
                                              &key initial-catalog connection-string
                                              database-type)) &body body)
  `(let ((,connection nil))
     (unwind-protect
         (progn 
           (setq ,connection (open-connection ,data-source ,user-id ,password
                                              :initial-catalog ,initial-catalog
                                              :connection-string ,connection-string
                                              :database-type ,database-type))
           ,@body)
       (ignore-errors
         (close-connection ,connection)))))

(defun get-getters (data-reader)
  (let ((data-reader (rdnzl:cast data-reader (object-type-of data-reader))))
    (loop for column below (rdnzl:property data-reader "FieldCount")
          for ct = (type-fullname (data-reader-get-field-type data-reader column))
          collect (cond ((string= ct "System.DateTime")
                         (lambda (r c)
                           (system-datetime-as-iso (data-reader-get-date-time r c))))
                        ((string= ct "System.String")
                         (lambda (r c) (data-reader-get-string r c)))
                        ((string= ct "System.Int32")
                         (lambda (r c) (data-reader-get-int32 r c)))
                        ((string= ct "System.Int64")
                         (lambda (r c) (data-reader-get-int64 r c)))
                        ((string= ct "System.Decimal")
                         (lambda (r c) (cast-system-decimal 
                                        (data-reader-get-decimal r c))))
                        ((string= ct "System.Float")
                         (lambda (r c) (data-reader-get-float r c)))
                        ((string= ct "System.Double")
                         (lambda (r c ) (data-reader-get-double r c)))
                        ((string= ct "System.Byte[]")
                         (lambda (r c) (system-convert-to-base64-string
                                        (rdnzl:cast (data-reader-get-other r c) "System.Byte[]"))))
                        (t
                         (lambda (r c) (rdnzl:invoke (data-reader-get-other r c) "ToString")))))))

(defun get-names (data-reader)
  (let ((data-reader (rdnzl:cast data-reader (object-type-of data-reader))))
    (loop for column below (rdnzl:property data-reader "FieldCount")
          collect (rdnzl:invoke data-reader "GetName" column))))

(defun setup-query (command query)
  (setf (rdnzl:property command "CommandTimeout") 300)
  (if (stringp query)
    (setf (command-command-text command) query)
    (destructuring-bind (query bindings)
        query
      (setf (command-command-text command) query)
      (let* ((par-object (rdnzl:property command "Parameters"))
             (par (rdnzl:cast par-object (object-type-of par-object))))
        (if (every #'listp bindings)
          (loop for (name value) in bindings
                do (rdnzl:invoke par "Add"
                                 name
                                 (if (and (numberp value) (zerop value))
                                   (rdnzl:invoke "System.Convert" "ToInt32" value)
                                   (rdnzl:cast (rdnzl:box value) "System.Object"))))
          (loop for value in bindings
                do (rdnzl:invoke par "Add" (rdnzl:cast (rdnzl:box value) "System.Object"))))))))

(defmacro do-query ((&rest args) query connection &body body)
  (let ((g-command (gensym "SQLCOMMAND-"))
        (g-reader (gensym "READER-"))
        (g-getters (gensym "GETTERS-"))
        (g-available-p (gensym "AVAILABLE-P"))
        (g-count (gensym "COUNT-"))
        (g-getter (gensym "GETTER-")))
    `(let* ((,g-command (connection-create-command ,connection)))
       (setup-query ,g-command ,query)
       (let* ((,g-reader (command-execute-reader ,g-command))
              (,g-getters (get-getters ,g-reader)))
         (unwind-protect
             (loop for ,g-available-p = (data-reader-read ,g-reader)
                   while ,g-available-p
                   do (destructuring-bind ,args
                          (loop for ,g-count from 0
                                for ,g-getter in ,g-getters
                                collect (and (not (data-reader-is-dbnull ,g-reader ,g-count))
                                             (funcall ,g-getter ,g-reader ,g-count)))
                        ,@body))
           (data-reader-close ,g-reader)
           (command-dispose ,g-command))))))

(defmacro do-query-with-getters ((&rest args) query connection getters &body body)
  (let ((g-command (gensym "SQLCOMMAND-"))
        (g-reader (gensym "READER-"))
        (g-available-p (gensym "AVAILABLE-P"))
        (g-count (gensym "COUNT-"))
        (g-getter (gensym "GETTER-")))
    `(let* ((,g-command (connection-create-command ,connection)))
       (setup-query ,g-command ,query)
       (let* ((,g-reader (command-execute-reader ,g-command)))
         (unless ,getters
           (setf ,getters (get-getters ,g-reader)))
         (unwind-protect
             (loop for ,g-available-p = (data-reader-read ,g-reader)
                   while ,g-available-p
                   do (destructuring-bind ,args
                          (loop for ,g-count from 0
                                for ,g-getter in ,getters
                                collect (and (not (data-reader-is-dbnull ,g-reader ,g-count))
                                             (funcall ,g-getter ,g-reader ,g-count)))
                        ,@body))
           (data-reader-close ,g-reader)
           (command-dispose ,g-command))))))

(defun execute-single (connection query)
  (let ((command (connection-create-command connection)))
    (setup-query command query)
    (unwind-protect
        (let ((obj 
               (command-execute-scalar command)))
          (and obj
               (let ((type-name (type-fullname (object-type-of obj))))
                 (cond ((string= type-name "System.Decimal")
                        (cast-system-decimal (rdnzl:cast obj "System.Decimal")))
                       ((string= type-name "System.Byte[]")
                        (rdnzl:unbox (system-convert-to-base64-string (rdnzl:cast obj "System.Byte[]"))))
                       (t
                        (rdnzl:cast obj type-name))))))
      (command-dispose command))))

(defun execute-non-query (connection query)
  (let ((command (connection-create-command connection)))
    (setup-query command query)
    (unwind-protect
        (command-execute-non-query command)
      (command-dispose command))))

(defmacro execute-to-list-with-columns (connection query)
  (let ((g-command (gensym "SQLCOMMAND-"))
        (g-reader (gensym "READER-"))
        (g-names (gensym "NAMES-"))
        (g-getters (gensym "GETTERS-"))
        (g-available-p (gensym "AVAILABLE-P"))
        (g-count (gensym "COUNT-"))
        (g-getter (gensym "GETTER-"))
        (g-res (gensym "RES-")))
    `(let* ((,g-command (connection-create-command ,connection)))
       (setup-query ,g-command ,query)
       (let* ((,g-reader (command-execute-reader ,g-command))
              (,g-getters (get-getters ,g-reader))
              (,g-names (get-names ,g-reader))
               (,g-res
               (unwind-protect
                   (loop for ,g-available-p = (data-reader-read ,g-reader)
                         while ,g-available-p
                         collect
                         (loop for ,g-count from 0
                               for ,g-getter in ,g-getters
                               collect (and (not (data-reader-is-dbnull ,g-reader ,g-count))
                                            (funcall ,g-getter ,g-reader ,g-count))))
                 (data-reader-close ,g-reader)
                 (command-dispose ,g-command))))
         (values ,g-res ,g-names)))))

(defun execute-to-list (connection query)
  (multiple-value-bind (res names)
      (execute-to-list-with-columns connection query)
    (declare (ignore names))
    res))

#+lw
(progn
  (editor:setup-indent "execute-non-query" 1 2 4)
  (editor:setup-indent "execute-single" 1 2 4)
  (editor:setup-indent "execute-to-list" 1 2 4)
  (editor:setup-indent "execute-to-list-with-columns" 1 2 4))

