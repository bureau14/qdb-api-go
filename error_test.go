package qdb

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tests", func() {

	Context("Errors", func() {
		It("success", func() {
			err := makeErrorOrNil(Success)
			Expect(err).To(BeNil())
		})
		It("Created", func() {
			err := makeErrorOrNil(Created)
			Expect(err).To(BeNil())
		})
		It("ErrUninitialized", func() {
			err := makeErrorOrNil(ErrUninitialized)
			Expect(err).To(BeNumerically("==", ErrUninitialized))
		})
		It("ErrAliasNotFound", func() {
			err := makeErrorOrNil(ErrAliasNotFound)
			Expect(err).To(BeNumerically("==", ErrAliasNotFound))
		})
		It("ErrAliasAlreadyExists", func() {
			err := makeErrorOrNil(ErrAliasAlreadyExists)
			Expect(err).To(BeNumerically("==", ErrAliasAlreadyExists))
		})
		It("ErrOutOfBounds", func() {
			err := makeErrorOrNil(ErrOutOfBounds)
			Expect(err).To(BeNumerically("==", ErrOutOfBounds))
		})
		It("ErrSkipped", func() {
			err := makeErrorOrNil(ErrSkipped)
			Expect(err).To(BeNumerically("==", ErrSkipped))
		})
		It("ErrIncompatibleType", func() {
			err := makeErrorOrNil(ErrIncompatibleType)
			Expect(err).To(BeNumerically("==", ErrIncompatibleType))
		})
		It("ErrContainerEmpty", func() {
			err := makeErrorOrNil(ErrContainerEmpty)
			Expect(err).To(BeNumerically("==", ErrContainerEmpty))
		})
		It("ErrContainerFull", func() {
			err := makeErrorOrNil(ErrContainerFull)
			Expect(err).To(BeNumerically("==", ErrContainerFull))
		})
		It("ErrElementNotFound", func() {
			err := makeErrorOrNil(ErrElementNotFound)
			Expect(err).To(BeNumerically("==", ErrElementNotFound))
		})
		It("ErrElementAlreadyExists", func() {
			err := makeErrorOrNil(ErrElementAlreadyExists)
			Expect(err).To(BeNumerically("==", ErrElementAlreadyExists))
		})
		It("ErrOverflow", func() {
			err := makeErrorOrNil(ErrOverflow)
			Expect(err).To(BeNumerically("==", ErrOverflow))
		})
		It("ErrUnderflow", func() {
			err := makeErrorOrNil(ErrUnderflow)
			Expect(err).To(BeNumerically("==", ErrUnderflow))
		})
		It("ErrTagAlreadySet", func() {
			err := makeErrorOrNil(ErrTagAlreadySet)
			Expect(err).To(BeNumerically("==", ErrTagAlreadySet))
		})
		It("ErrTagNotSet", func() {
			err := makeErrorOrNil(ErrTagNotSet)
			Expect(err).To(BeNumerically("==", ErrTagNotSet))
		})
		It("ErrTimeout", func() {
			err := makeErrorOrNil(ErrTimeout)
			Expect(err).To(BeNumerically("==", ErrTimeout))
		})
		It("ErrConnectionRefused", func() {
			err := makeErrorOrNil(ErrConnectionRefused)
			Expect(err).To(BeNumerically("==", ErrConnectionRefused))
		})
		It("ErrConnectionReset", func() {
			err := makeErrorOrNil(ErrConnectionReset)
			Expect(err).To(BeNumerically("==", ErrConnectionReset))
		})
		It("ErrUnstableCluster", func() {
			err := makeErrorOrNil(ErrUnstableCluster)
			Expect(err).To(BeNumerically("==", ErrUnstableCluster))
		})
		It("ErrTryAgain", func() {
			err := makeErrorOrNil(ErrTryAgain)
			Expect(err).To(BeNumerically("==", ErrTryAgain))
		})
		It("ErrConflict", func() {
			err := makeErrorOrNil(ErrConflict)
			Expect(err).To(BeNumerically("==", ErrConflict))
		})
		It("ErrNotConnected", func() {
			err := makeErrorOrNil(ErrNotConnected)
			Expect(err).To(BeNumerically("==", ErrNotConnected))
		})
		It("ErrResourceLocked", func() {
			err := makeErrorOrNil(ErrResourceLocked)
			Expect(err).To(BeNumerically("==", ErrResourceLocked))
		})
		It("ErrSystemRemote", func() {
			err := makeErrorOrNil(ErrSystemRemote)
			Expect(err).To(BeNumerically("==", ErrSystemRemote))
		})
		It("ErrSystemLocal", func() {
			err := makeErrorOrNil(ErrSystemLocal)
			Expect(err).To(BeNumerically("==", ErrSystemLocal))
		})
		It("ErrInternalRemote", func() {
			err := makeErrorOrNil(ErrInternalRemote)
			Expect(err).To(BeNumerically("==", ErrInternalRemote))
		})
		It("ErrInternalLocal", func() {
			err := makeErrorOrNil(ErrInternalLocal)
			Expect(err).To(BeNumerically("==", ErrInternalLocal))
		})
		It("ErrNoMemoryRemote", func() {
			err := makeErrorOrNil(ErrNoMemoryRemote)
			Expect(err).To(BeNumerically("==", ErrNoMemoryRemote))
		})
		It("ErrNoMemoryLocal", func() {
			err := makeErrorOrNil(ErrNoMemoryLocal)
			Expect(err).To(BeNumerically("==", ErrNoMemoryLocal))
		})
		It("ErrInvalidProtocol", func() {
			err := makeErrorOrNil(ErrInvalidProtocol)
			Expect(err).To(BeNumerically("==", ErrInvalidProtocol))
		})
		It("ErrHostNotFound", func() {
			err := makeErrorOrNil(ErrHostNotFound)
			Expect(err).To(BeNumerically("==", ErrHostNotFound))
		})
		It("ErrBufferTooSmall", func() {
			err := makeErrorOrNil(ErrBufferTooSmall)
			Expect(err).To(BeNumerically("==", ErrBufferTooSmall))
		})
		It("ErrNotImplemented", func() {
			err := makeErrorOrNil(ErrNotImplemented)
			Expect(err).To(BeNumerically("==", ErrNotImplemented))
		})
		It("ErrInvalidVersion", func() {
			err := makeErrorOrNil(ErrInvalidVersion)
			Expect(err).To(BeNumerically("==", ErrInvalidVersion))
		})
		It("ErrInvalidArgument", func() {
			err := makeErrorOrNil(ErrInvalidArgument)
			Expect(err).To(BeNumerically("==", ErrInvalidArgument))
		})
		It("ErrInvalidHandle", func() {
			err := makeErrorOrNil(ErrInvalidHandle)
			Expect(err).To(BeNumerically("==", ErrInvalidHandle))
		})
		It("ErrReservedAlias", func() {
			err := makeErrorOrNil(ErrReservedAlias)
			Expect(err).To(BeNumerically("==", ErrReservedAlias))
		})
		It("ErrUnmatchedContent", func() {
			err := makeErrorOrNil(ErrUnmatchedContent)
			Expect(err).To(BeNumerically("==", ErrUnmatchedContent))
		})
		It("ErrInvalidIterator", func() {
			err := makeErrorOrNil(ErrInvalidIterator)
			Expect(err).To(BeNumerically("==", ErrInvalidIterator))
		})
		It("ErrEntryTooLarge", func() {
			err := makeErrorOrNil(ErrEntryTooLarge)
			Expect(err).To(BeNumerically("==", ErrEntryTooLarge))
		})
		It("ErrTransactionPartialFailure", func() {
			err := makeErrorOrNil(ErrTransactionPartialFailure)
			Expect(err).To(BeNumerically("==", ErrTransactionPartialFailure))
		})
		It("ErrOperationDisabled", func() {
			err := makeErrorOrNil(ErrOperationDisabled)
			Expect(err).To(BeNumerically("==", ErrOperationDisabled))
		})
		It("ErrOperationNotPermitted", func() {
			err := makeErrorOrNil(ErrOperationNotPermitted)
			Expect(err).To(BeNumerically("==", ErrOperationNotPermitted))
		})
		It("ErrIteratorEnd", func() {
			err := makeErrorOrNil(ErrIteratorEnd)
			Expect(err).To(BeNumerically("==", ErrIteratorEnd))
		})
		It("ErrInvalidReply", func() {
			err := makeErrorOrNil(ErrInvalidReply)
			Expect(err).To(BeNumerically("==", ErrInvalidReply))
		})
		It("ErrNoSpaceLeft", func() {
			err := makeErrorOrNil(ErrNoSpaceLeft)
			Expect(err).To(BeNumerically("==", ErrNoSpaceLeft))
		})
		It("ErrQuotaExceeded", func() {
			err := makeErrorOrNil(ErrQuotaExceeded)
			Expect(err).To(BeNumerically("==", ErrQuotaExceeded))
		})
		It("ErrAliasTooLong", func() {
			err := makeErrorOrNil(ErrAliasTooLong)
			Expect(err).To(BeNumerically("==", ErrAliasTooLong))
		})
		It("ErrClockSkew", func() {
			err := makeErrorOrNil(ErrClockSkew)
			Expect(err).To(BeNumerically("==", ErrClockSkew))
		})
		It("ErrAccessDenied", func() {
			err := makeErrorOrNil(ErrAccessDenied)
			Expect(err).To(BeNumerically("==", ErrAccessDenied))
		})
		It("ErrLoginFailed", func() {
			err := makeErrorOrNil(ErrLoginFailed)
			Expect(err).To(BeNumerically("==", ErrLoginFailed))
		})
		It("ErrColumnNotFound", func() {
			err := makeErrorOrNil(ErrColumnNotFound)
			Expect(err).To(BeNumerically("==", ErrColumnNotFound))
		})
		It("ErrQueryTooComplex", func() {
			err := makeErrorOrNil(ErrQueryTooComplex)
			Expect(err).To(BeNumerically("==", ErrQueryTooComplex))
		})
		It("ErrInvalidCryptoKey", func() {
			err := makeErrorOrNil(ErrInvalidCryptoKey)
			Expect(err).To(BeNumerically("==", ErrInvalidCryptoKey))
		})
	})
})
