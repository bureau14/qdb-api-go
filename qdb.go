package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/blob.h>
	#include <qdb/error.h>
	#include <qdb/client.h>
	#include <string.h>

   int blob_put(const char *alias, const char *content)
   {
		qdb_handle_t handle;
		qdb_error_t error = qdb_open(&handle, qdb_p_tcp);
		if (error) return -1;

		error = qdb_connect(handle, "qdb://localhost:2836");
		if (error)
		{ return -1; }

		error = qdb_blob_put(handle, alias, (const void *)content, strlen(content), qdb_never_expires);
		if (error)
		{
			return -1;
		}
   }
*/
import "C"
import (
	"fmt"
)

// export "BlobPut ..."
func BlobPut(alias string, content string) {
	fmt.Println(int(C.blob_put(C.CString(alias), C.CString(content))))
}
