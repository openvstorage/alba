;;; doc_generation --- persuade emacs to do it for me

(with-current-buffer (find-file "./doc/architecture.org")
  (org-latex-export-to-pdf )
  )
