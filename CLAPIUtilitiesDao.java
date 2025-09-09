package com.focus.erp.crm.api.dao.util;

import com.focus.erp.crm.CLServiceLocator;
import com.focus.erp.crm.api.dao.common.dto.CLAPIMailMessageDTO;
import com.focus.erp.crm.api.dao.common.dto.CLAPIMessagesDTO;
import com.focus.erp.crm.api.dao.common.dto.CLAPIModuleSMSDTO;
import com.focus.erp.crm.api.dao.common.dto.CLAPISMSMessageDTO;
import com.focus.erp.crm.api.dao.module.dto.*;
import com.focus.erp.crm.api.dao.util.dto.CLAPISurveyResponseDTO;
import com.focus.erp.crm.api.dto.CLStatusResponseDTO;
import com.focus.erp.crm.api.errors.CLErrorDTO;
import com.focus.erp.crm.api.errors.CLFieldErrorDTO;
import com.focus.erp.crm.business.dao.CLBaseDao;
import com.focus.erp.crm.business.dao.CLProcessDocuments;
import com.focus.erp.crm.business.dao.IConstants;
import com.focus.erp.crm.business.dao.IMessageCodes;
import com.focus.erp.crm.business.dao.common.ICommunicationDao;
import com.focus.erp.crm.business.dao.common.IEmailDao;
import com.focus.erp.crm.business.dao.common.company.ICompanyViewDao;
import com.focus.erp.crm.business.dao.common.dto.*;
import com.focus.erp.crm.business.dao.module.dto.CLBodyValuesDTO;
import com.focus.erp.crm.business.dao.module.dto.CLFieldValueDTO;
import com.focus.erp.crm.business.service.holder.CLKeyValueSII;
import com.focus.erp.crm.business.dao.common.dto.CLKeyValueSS;
import com.focus.erp.crm.business.dao.exception.CLBusinessRuleException;
import com.focus.erp.crm.business.dao.exception.CLDaoException;
import com.focus.erp.crm.business.dao.module.IActivityViewDao;
import com.focus.erp.crm.business.dao.module.IModule;
import com.focus.erp.crm.business.dao.module.IReminderDao;
import com.focus.erp.crm.business.dao.module.ISearchLayout;
import com.focus.erp.crm.business.dao.module.dto.CLReminderInfoDTO;
import com.focus.erp.crm.business.dao.module.layout.IPrintLayoutDao;
import com.focus.erp.crm.business.dao.pms.IUnitConversionDao;
import com.focus.erp.crm.business.dao.pms.dto.CLUnitBodyDTO;
import com.focus.erp.crm.business.dao.pms.dto.CLUnitConversionDTO;
import com.focus.erp.crm.business.dao.report.dto.CLRepFilterDTO;
import com.focus.erp.crm.business.dao.report.external.IViewerDao;
import com.focus.erp.crm.business.dao.security.CLLoginJMSCallback;
import com.focus.erp.crm.business.dao.security.IUserDao;
import com.focus.erp.crm.business.dao.security.dto.CLUserBaseDTO;
import com.focus.erp.crm.business.dao.survey.ISurveyConstants;
import com.focus.erp.crm.business.dao.survey.ISurveyDao;
import com.focus.erp.crm.business.dao.survey.ISurveyDesignerDao;
import com.focus.erp.crm.business.dao.survey.dto.CLSurveyDTO;
import com.focus.erp.crm.business.engine.*;
import com.focus.erp.crm.business.service.holder.*;
import com.focus.erp.crm.business.service.module.CLModuleResolver;
import com.focus.erp.crm.controller.common.helper.CLResourceHlp;
import com.focus.erp.crm.datalayer.connection.CLSqlBase;
import com.focus.erp.crm.util.CLUtilities;
import com.focus.erp.crm.webcontext.app.CLApplicationContext;
import com.focus.erp.crm.webcontext.company.ICompanyDTO;
import com.focus.erp.crm.webcontext.session.IBaseSessionDTO;
import com.focus.erp.crm.webcontext.session.holder.CLCSSSessionDTO;
import com.focus.erp.jms.core.IMessage;
import com.focus.erp.jms.core.callback.IJMSCallback;
//import org.apache.log4j.Logger;
import com.focus.erp.logging.CLLogManager;
import org.slf4j.Logger;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * Created by raghu on 20-04-2016.
 */
public class CLAPIUtilitiesDao extends CLBaseDao implements IAPIUtilitiesDao, IJMSCallback
{
    private static Logger logger = CLLogManager.getLogger(CLAPIUtilitiesDao.class);

    @Override
    public CLResultDTO getUserInfo(int iUserId,boolean bIsCSSUser)
    {
        CLResultDTO clResultDTO = new CLResultDTO();
        try
        {
            if(!bIsCSSUser)
            {
                IUserDao clUserDao = (IUserDao) CLServiceLocator.getDaoBean("IUserDao");
                Object[] objUserProfile = clUserDao.getUserProfileDetails(iUserId);
                if (objUserProfile != null)
                {
                    CLUserBaseDTO clUserBaseDTO = (CLUserBaseDTO) objUserProfile[0];
                    LinkedHashMap clLinkedHashMap = new LinkedHashMap();
                    clLinkedHashMap.put("userId", CLUtilities.getAPITransId(iUserId,IConstants.IModule.ITypes.USERS,0));
                    clLinkedHashMap.put("name", clUserBaseDTO.getFirstName());
                    clLinkedHashMap.put("email", clUserBaseDTO.getEmail());
                    clLinkedHashMap.put("image", clUserBaseDTO.getImageFileName());
                    clResultDTO.setCount(1);
                    clResultDTO.setStatus(IMessageCodes.MSG_SUCCESS);
                    ArrayList alCompanyDetails = new ArrayList();
                    alCompanyDetails.add(clLinkedHashMap);
                    clResultDTO.setRecords(alCompanyDetails);

                }
            }
            else
            {
                clResultDTO = getContactDetails(iUserId);
            }

        }
        catch (Exception e)
        {
            clResultDTO.setCount(0);
            clResultDTO.setStatus(IMessageCodes.MSG_FAIL);
            CLErrorDTO clErrorDTO = new CLErrorDTO();
            clErrorDTO.setMessage(e.getMessage());
            ArrayList<CLErrorDTO> clErrorDTOs= new ArrayList<CLErrorDTO>();
            clErrorDTOs.add(clErrorDTO);
            clResultDTO.setErrors(clErrorDTOs);
            logger.error("",e);
        }
        return clResultDTO;
    }

    @Override
    public int savePaymentTokenDetails(String sInternalTokenId) throws CLDaoException
    {
        int iPaymentCallId=0;
        try
        {
            initSqlBase();
            CLSqlBase clSqlBase = getSqlBase();
            Statement stmt = clSqlBase.getStatement();

            ResultSet rs = stmt.executeQuery(" select iAccessTokenId from mCrm_ApiUserAccessToken where sAccessToken =  '"+sInternalTokenId+"'");
            int iAccessTokenId = 0;
            if(rs.next())
                iAccessTokenId = rs.getInt(1);
            rs.close();
            stmt.executeUpdate("insert into tCrm_ApiPaymentGatewayCalls(iAccessTokenId) values(" + iAccessTokenId + ")", Statement.RETURN_GENERATED_KEYS);
            rs = stmt.getGeneratedKeys();
            rs.next();
            iPaymentCallId = rs.getInt(1);
            rs.close();

        }
        catch (CLBusinessRuleException bre)
        {
            throw bre;
        }
        catch (Exception e)
        {
            logger.error("", e);
        }
        finally
        {
            releaseSqlBase();
        }
        return iPaymentCallId;
    }

    @Override
    public Object[] getPaymentTokenDetails(int iPaymentCallId) throws CLDaoException
    {
        Object [] objTokenDetails = new Object[3];
        try
        {
            initSqlBase();
            CLSqlBase clSqlBase = getSqlBase();
            Statement stmt = clSqlBase.getStatement();

            StringBuilder sbQuery = new StringBuilder();
            sbQuery.append(" select sAccessToken,iUserId,iClientId from tCrm_ApiPaymentGatewayCalls tcpg ");
            sbQuery.append(" inner join mCrm_ApiUserAccessToken mca on tcpg.iAccessTokenId=mca.iAccessTokenId ");
            sbQuery.append(" where iPaymentCallId=" + iPaymentCallId);

            ResultSet rs = stmt.executeQuery(sbQuery.toString());

            if(rs.next())
            {
                objTokenDetails[0] = rs.getString(1);
                objTokenDetails[1] = rs.getInt(2);
                objTokenDetails[2] = rs.getInt(3);
            }
            rs.close();
        }
        catch (CLBusinessRuleException bre)
        {
            throw bre;
        }
        catch (Exception e)
        {
            logger.error("", e);
        }
        finally
        {
            releaseSqlBase();
        }
        return objTokenDetails;
    }

    @Override
    public CLStatusResponseDTO sendMail(CLAPIMessagesDTO clapiMessagesDTO, String[] sTempFilePaths)
    {
        CLStatusResponseDTO clStatusResponseDTO = new CLStatusResponseDTO();
        try
        {
            initSqlBase();
            CLSqlBase clSqlBase = getSqlBase();
            IBaseSessionDTO clSessionDTO = CLApplicationContext.getInstance().getSessionDTO();
            ArrayList<CLAPIMailMessageDTO> clapiMailMessageDTOs = clapiMessagesDTO.getMessages();
            CLAPIMailMessageDTO clapiMailMessageDTO = null;
            ICommunicationDao clCommunicationDao = (ICommunicationDao) CLServiceLocator.getDaoBean("ICommunicationDao");
            clCommunicationDao.setMultiTrans(true);
            clCommunicationDao.setSqlBase(clSqlBase);
            String sSavePath = CLResourceHlp.DEFAULT_PATH + File.separator + IConstants.ResourceTypes.EMAILS.getDestinationFolder() + File.separator + clSessionDTO.getCompanyCode();
            String[] sFilesPaths = null, sFilesPaths2 = null,sAttachedFileNames=null;
            String sTempFileName = null,sPassword = null, sFromAddress = null,sSubject=null,sBody=null;
            int iTemplateId=0,iModuleId=0,iUserId=0,iRoleId=0,iMasterId=0;
            CLKeyValueSS clFromAdress = null;
            ArrayList clAddress =null;
            CLModuleMessageDTO clMailMessage =null;
            for (int i = 0; i < clapiMailMessageDTOs.size(); i++)
            {
                clapiMailMessageDTO = clapiMailMessageDTOs.get(i);
                if(clapiMailMessageDTO.getModuleName()==null)//Just Plain main with subject and body only
                    sendPlainMail(clapiMailMessageDTO);
                else
                {
                    sFilesPaths2 = clapiMailMessageDTO.getAttachments();
                    if (sFilesPaths2 != null && sTempFilePaths != null && (sTempFilePaths.length > sFilesPaths2.length))
                    {
                        sFilesPaths = new String[sFilesPaths2.length];
                        for (int j = 0, l = 0; j < sTempFilePaths.length; j++)
                        {
                            sTempFileName = sTempFilePaths[j];
                            if (sTempFileName != null && sTempFileName.trim().length() > 0)
                            {
                                sTempFileName = sTempFileName.substring(sTempFileName.lastIndexOf("_") + 1);
                                for (int k = 0; k < sFilesPaths2.length; k++)
                                {
                                    if (sTempFileName.equalsIgnoreCase(sFilesPaths2[k]))
                                    {
                                        sFilesPaths[l++] = sTempFilePaths[j];
                                    }
                                }
                            }
                        }
                    }
//                else sFilesPaths=sFilesPaths2;
                    ICompanyDTO clCompanyDTO = (CLApplicationContext.getInstance().getCompanyDTO());
                    IBaseSessionDTO clBaseSessionDTO = CLApplicationContext.getInstance().getSessionDTO();
//                int iModuleId=clCompanyDTO.getModuleId(clapiMailMessageDTO.getModuleName());
//                iTemplateId=0;iModuleId=0;iUserId=0;iRoleId=0;
                    iModuleId = clCompanyDTO.getModuleIdFromAPIName(clapiMailMessageDTO.getModuleName());
                    iUserId = clBaseSessionDTO.getLoginId();
                    iRoleId = clBaseSessionDTO.getRoleId();
                    iTemplateId = clapiMailMessageDTO.getTemplateId();
                    if (iTemplateId <= 0)
                    {
                        iTemplateId = clCommunicationDao.getTemplateId(clapiMailMessageDTO.getTemplateName());
                    }
                    iMasterId = CLUtilities.getAPITransIdToInt(clapiMailMessageDTO.getMasterId(), 0);

                    sPassword = null;
                    sFromAddress = null;
                    clAddress = clCommunicationDao.getFromEmail(iRoleId, iModuleId, iUserId);
                    if (clAddress != null && clAddress.size() > 0)
                    {
                        Object[] objArrDets = (Object[]) clAddress.get(0);
                        clFromAdress = (CLKeyValueSS) objArrDets[0];
//                    clFromAdress = (CLKeyValueSS) clAddress.get(0);
                        sFromAddress = clFromAdress.getStrKey();
                        sPassword = CLUtilities.decrypt(clFromAdress.getStrValue());
                    }
                    sSubject = clapiMailMessageDTO.getSubject();
                    sBody = clapiMailMessageDTO.getBody();
                    if (iTemplateId > 0)
                    {
                        Object objAttachments[] = clCommunicationDao.getTemplateBasedAttachments(iTemplateId, iMasterId, sFilesPaths, 0, null);
                        iModuleId = Integer.parseInt(String.valueOf(objAttachments[3]));
                        sFilesPaths = (String[]) objAttachments[4];
                        sAttachedFileNames = (String[]) objAttachments[5];

                        if (sSubject == null || sSubject.trim().length() <= 0 || sBody == null || sBody.trim().length() <= 0)
                        {
                            Object[] objReturnData = clCommunicationDao.getMessageContent(iTemplateId, iUserId, clSessionDTO.getAdminAccess(), clCompanyDTO.getCompanyName(), iMasterId, iModuleId, 0, IConstants.ITemplates.EMAIL, null, (byte) 0);
                            sSubject = (String) objReturnData[0];
                            sBody = (String) objReturnData[1];
                        }
                    }


                    clMailMessage = new CLModuleMessageDTO(clCompanyDTO.getCompanyId());
                    clMailMessage.setCallback(this);
                    clMailMessage.setEmailAddr(clapiMailMessageDTO.getTo());
                    clMailMessage.setFromAddress(sFromAddress, sPassword);
                    clMailMessage.setSubject(sSubject);
                    clMailMessage.setMessage(sBody);
                    clMailMessage.setCC(clapiMailMessageDTO.getCc());
                    clMailMessage.setBCC(clapiMailMessageDTO.getBcc());
                    clMailMessage.setSavePath(sSavePath);
                    clMailMessage.setSessionTaggedToFile(true);
                    clMailMessage.setAttachmentPath(sFilesPaths);
                    if (sAttachedFileNames != null) clMailMessage.setAttachmentNames(sAttachedFileNames);
                    clMailMessage.setFailureUrl(clapiMailMessageDTO.getFailureUrl());
                    clMailMessage.setSuccessUrl(clapiMailMessageDTO.getSuccessUrl());
                    clMailMessage.setReferKey(clapiMailMessageDTO.getReferKey());
                    clCommunicationDao.sendMail(clMailMessage, IConstants.CommunicationErrors.RaiseException.getCommunicationError());
                    clStatusResponseDTO.setStatus((short) 1);
                    clStatusResponseDTO.setMessage("Mail has been processed");
                }
            }
        }
        catch (CLBusinessRuleException bre)
        {
            clStatusResponseDTO.setStatus((short) 0);
            CLErrorDTO clErrorDTO = new CLErrorDTO();
            clErrorDTO.setErrorCode(bre.getMsgCode());
            clErrorDTO.setMessage(bre.getMessage());
            ArrayList<CLErrorDTO> clErrorDTOs = new ArrayList<CLErrorDTO>();
            clErrorDTOs.add(clErrorDTO);
            clStatusResponseDTO.setErrors(clErrorDTOs);
        }
        catch (Exception e)
        {
            clStatusResponseDTO.setStatus((short) 0);
            CLErrorDTO clErrorDTO = new CLErrorDTO();
            clErrorDTO.setMessage(e.getMessage());
            ArrayList<CLErrorDTO> clErrorDTOs = new ArrayList<CLErrorDTO>();
            clErrorDTOs.add(clErrorDTO);
            clStatusResponseDTO.setErrors(clErrorDTOs);
            logger.error("", e);
        }
        finally
        {
            releaseSqlBase();
        }
        return clStatusResponseDTO;
    }

    @Override
    public void onMessageResponse(IMessage clIMessage, byte byStatus, String sStatusMsg)
    {
        CLModuleMessageDTO clModuleMessageDTO = (CLModuleMessageDTO) clIMessage;
        if((clModuleMessageDTO.getSuccessUrl()!=null && clModuleMessageDTO.getSuccessUrl().trim().length()>0)
            || (clModuleMessageDTO.getFailureUrl()!=null && clModuleMessageDTO.getFailureUrl().trim().length()>0))
        {
            if (byStatus != com.focus.erp.jms.provider.mail.IConstants.EMAIL_ERRORCODES.FAILED_TO_SEND.getCode())
            {
                getURLConnection(clModuleMessageDTO.getSuccessUrl());
            }
            else
            {
                getURLConnection(clModuleMessageDTO.getFailureUrl());
            }
        }
    }

    private String getURLConnection(String sUrl)
    {
        String sUrlString = "";
        try
        {
            URL url = new URL(sUrl);
            URLConnection urlConnection = url.openConnection();
            HttpURLConnection connection = null;
            if(urlConnection instanceof HttpURLConnection)
            {
                connection = (HttpURLConnection) urlConnection;
            }
            else
            {
                return null;
            }
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));

            String current;
            while((current = in.readLine()) != null)
            {
                sUrlString += current;
            }
        }
        catch(IOException e)
        {
            logger.error("",e);
            e.printStackTrace();
        }
        return sUrlString;
    }

    public Object[] getSurveyList(String sModuleName,long lTransId,int iTypeId) throws CLDaoException
    {
        CLResultDTO clResultDTO = new CLResultDTO();
        try
        {
            ICompanyDTO clCompanyDTO = (CLApplicationContext.getInstance().getCompanyDTO() );
            int iTransId=0,iModuleId =0;
//            clCompanyDTO.getModuleIdFromAPIName();
            iModuleId = clCompanyDTO.getModuleIdFromAPIName(sModuleName);
//            iModuleId = clCompanyDTO.getModuleId(sModuleName);
            if (iModuleId == 0)
                throw new CLBusinessRuleException(IMessageCodes.MSG_INVALID_MODULE_NAME);
            if (lTransId > 0)
                iTransId = CLUtilities.getAPITransIdToInt(lTransId, 0);
            IBaseSessionDTO clBaseSessionDTO = CLApplicationContext.getInstance().getSessionDTO();
            byte byMode = 0;
            byte byUserType = 0;
            if(clBaseSessionDTO instanceof CLCSSSessionDTO)
            {
//                CLCSSSessionDTO clCSSSessionDTO= (CLCSSSessionDTO)clBaseSessionDTO;
                iModuleId= IConstants.IModule.ITypes.CALLS;
                iTypeId= com.focus.erp.crm.business.dao.IConstants.IModule.ITypes.CONTACTS;
                byMode= ISurveyConstants.ISurvey_Assignment_Mode.MOBILEAPP;
                byUserType= com.focus.erp.crm.business.dao.IConstants.ISecurity.IUSERTYPES.CUSTOMER;
            }


            ISurveyDesignerDao clSurveyDesignerDao = (ISurveyDesignerDao) CLServiceLocator.getDaoBean("ISurveyDesignerDao");
            Object[] objSurvey = clSurveyDesignerDao.getSurveyDetails(iModuleId, iTransId, iTypeId, byMode, byUserType);
            Object[] objServeyData=null;
            if(objSurvey!=null)
            {
                ArrayList<CLAPISurveyResponseDTO> clapiSurveyResponseDTOs = new ArrayList<CLAPISurveyResponseDTO>();
                CLAPISurveyResponseDTO clapiSurveyResponseDTO=null;
                objSurvey = (Object[]) objSurvey[0];
                for (int i = 0; i < objSurvey.length; i++)
                {
                    objServeyData= (Object[]) objSurvey[i];
                    clapiSurveyResponseDTO = new CLAPISurveyResponseDTO(CLUtilities.getAPITransId(Integer.parseInt(String.valueOf(objServeyData[0])),IConstants.IModule.ITypes.SURVEYS,0),String.valueOf(objServeyData[1]),String.valueOf(objServeyData[14]),String.valueOf(objServeyData[11]));
                    clapiSurveyResponseDTOs.add(clapiSurveyResponseDTO);
                }
                clResultDTO.setStatus(IMessageCodes.MSG_SUCCESS);
                clResultDTO.setCount(clapiSurveyResponseDTOs.size());
                clResultDTO.setRecords(clapiSurveyResponseDTOs);
            }
        }
        catch (Exception e)
        {
            logger.error("",e);
            e.printStackTrace();
            clResultDTO.setCount(0);
            clResultDTO.setStatus((short) 0);
            ArrayList<CLErrorDTO> clErrorDTOs = new ArrayList<CLErrorDTO>();
            CLErrorDTO clErrorDTO = new CLErrorDTO();
            clErrorDTO.setMessage(e.getMessage() == null ? e.toString() : e.getMessage());
            clErrorDTOs.add(clErrorDTO);
            clResultDTO.setErrors(clErrorDTOs);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
        }
        finally
        {
            setMultiTrans(false);
            releaseSqlBase();
        }
        return new Object[]{clResultDTO};
    }

    public CLSurveyDTO getSurvey(int iSurveyId) throws CLDaoException
    {
        try
        {
            ISurveyDao clSurveyDao= (ISurveyDao) CLServiceLocator.getDaoBean("ISurveyDao");
            return clSurveyDao.getSurvey(iSurveyId);
        }
        catch (Exception e)
        {
            logger.error("", e);
            e.printStackTrace();
        }
        return null;
    }

    public CLResultDTO getCompanyDetails() throws CLDaoException
    {
        CLResultDTO clResultDTO=new CLResultDTO();
        try
        {
//            ICompanyDao clCompanyDao =(ICompanyDao)CLServiceLocator.getDaoBean("ICompanyDao");
            ICompanyViewDao clCompanyDao =(ICompanyViewDao)CLServiceLocator.getDaoBean("ICompanyViewDao");
            Object[] objCompanyDetails = clCompanyDao.getCompanyProfile();
            if(objCompanyDetails!=null)
            {
//                ISearchLayout clSearchLayout = (ISearchLayout) CLServiceLocator.getDaoBean("ISearchLayoutDao");
                CLCompanyProfileDTO clCompanyProfileDTO= (CLCompanyProfileDTO) objCompanyDetails[0];
                LinkedHashMap clLinkedHashMap = new LinkedHashMap();
                clLinkedHashMap.put("name", clCompanyProfileDTO.getCompanyName());
                clLinkedHashMap.put("address1", clCompanyProfileDTO.getBillAddress1());
                clLinkedHashMap.put("address2", clCompanyProfileDTO.getBillAddress2());
                clLinkedHashMap.put("city", clCompanyProfileDTO.getBillCity());
                clLinkedHashMap.put("zip", clCompanyProfileDTO.getBillZip());
                clLinkedHashMap.put("state", clCompanyProfileDTO.getBillState());
                clLinkedHashMap.put("country", clCompanyProfileDTO.getBillCountry());
                clLinkedHashMap.put("phone", clCompanyProfileDTO.getPhoneNumber());
                clResultDTO.setCount(1);
                clResultDTO.setStatus(IMessageCodes.MSG_SUCCESS);
                ArrayList alCompanyDetails = new ArrayList();
                alCompanyDetails.add(clLinkedHashMap);
                clResultDTO.setRecords(alCompanyDetails);
            }
        }
        catch (Exception e)
        {
            logger.info("",e);
            clResultDTO.setCount(0);
            clResultDTO.setStatus(IMessageCodes.MSG_FAIL);
            CLErrorDTO clErrorDTO = new CLErrorDTO();
            clErrorDTO.setMessage(e.getMessage());
            ArrayList<CLErrorDTO> clErrorDTOs= new ArrayList<CLErrorDTO>();
            clErrorDTOs.add(clErrorDTO);
            clResultDTO.setErrors(clErrorDTOs);
            throw new CLDaoException(e);
        }
        return clResultDTO;
    }

    public CLResultDTO getRefreshToken(String sAccessToken) throws CLDaoException
    {
        CLResultDTO clResultDTO=new CLResultDTO();
        try
        {
            initSqlBase();
            CLSqlBase clSqlBase = getSqlBase();
            CLTokenInfoDTO clTokenInfoDTO =CLTokenGenerator.parseToken(sAccessToken);
            String sRefreshToken=CLTokenGenerator.getRefreshToken(clSqlBase, clTokenInfoDTO.getInternalTokenId(), clTokenInfoDTO.getClientId());
            LinkedHashMap clLinkedHashMap = new LinkedHashMap();
            clLinkedHashMap.put("refreshToken", sRefreshToken);
            clResultDTO.setCount(1);
            clResultDTO.setStatus(IMessageCodes.MSG_SUCCESS);
            ArrayList alTokenDetails = new ArrayList();
            alTokenDetails.add(clLinkedHashMap);
            clResultDTO.setRecords(alTokenDetails);
        }
        catch (Exception e)
        {
            logger.info("",e);
            clResultDTO.setCount(0);
            clResultDTO.setStatus(IMessageCodes.MSG_FAIL);
            CLErrorDTO clErrorDTO = new CLErrorDTO();
            clErrorDTO.setMessage(e.getMessage());
            ArrayList<CLErrorDTO> clErrorDTOs= new ArrayList<CLErrorDTO>();
            clErrorDTOs.add(clErrorDTO);
            clResultDTO.setErrors(clErrorDTOs);
            throw new CLDaoException(e);
        }
        return clResultDTO;
    }

    private CLResultDTO getContactDetails(int iContactId) throws CLDaoException
    {
        CLResultDTO clResultDTO = new CLResultDTO();
        try
        {
            initSqlBase();
            CLSqlBase clSqlBase = getSqlBase();
            Statement stmt = clSqlBase.getStatement();
            ResultSet rs = stmt.executeQuery(" select vcc.sName,vcc.sPhone,vcc.sEmail,tcci.binImage from vCrm_Contacts vcc \n" +
                    "left outer join tCrm_ContactImages tcci on vcc.iMasterId=tcci.iContactId where vcc.iMasterId=" + iContactId);

//            rs = stmt.executeQuery(sbQuery.toString());
            String sImageName=null;
            if(rs.next())
            {
                LinkedHashMap clLinkedHashMap = new LinkedHashMap();
                clLinkedHashMap.put("Name", rs.getString(1));
                clLinkedHashMap.put("Phone", rs.getString(2));
                clLinkedHashMap.put("Email", rs.getString(3));
                sImageName=rs.getString(4);
                if(sImageName!=null && sImageName.trim().length()>0)
                {
                    LinkedHashMap clLinkedHashMap2 = new LinkedHashMap();
                    clLinkedHashMap2.put("name", rs.getString(4));
                    clLinkedHashMap2.put("url", "/crmservices/rest/modules/v1.0/documents/"+ CLUtilities.getAPITransId(iContactId, IConstants.IModule.ITypes.CONTACTS, 0));
//                    clLinkedHashMap2.put("url", "/crmservices/uploadImage.action?moduleId=" + IConstants.IModule.ITypes.CONTACTS + "&transId=" + iContactId);
                    clLinkedHashMap.put("Image", clLinkedHashMap2);
                }
                clResultDTO.setCount(1);
                clResultDTO.setStatus(IMessageCodes.MSG_SUCCESS);
                ArrayList alCompanyDetails = new ArrayList();
                alCompanyDetails.add(clLinkedHashMap);
                clResultDTO.setRecords(alCompanyDetails);
            }
            rs.close();
/*            sbQuery.append("select sAPIName from vCrm_Fields where iTypeId="+iModuleTypeId+" and iDataTypeId="+IConstants.FieldCustomization.DATATYPE_PICTURE);
            rs=stmt.executeQuery(sbQuery.toString());
            if(rs.next())
                sPictureFldName=rs.getString(1);
            rs.close();
            sbQuery.setLength(0);
            sbQuery.append(" select vcc.sName,vcc.sPhone,vcc.sEmail"+(sPictureFldName!=null?","+sPictureFldName+"Name":"")+" from vCrm_Contacts vcc where vcc.iMasterId=" + iContactId);

            rs = stmt.executeQuery(sbQuery.toString());
            String sImageName=null;
            if(rs.next())
            {
                LinkedHashMap clLinkedHashMap = new LinkedHashMap();
                clLinkedHashMap.put("Name", rs.getString(1));
                clLinkedHashMap.put("Phone", rs.getString(2));
                clLinkedHashMap.put("Email", rs.getString(3));
                sImageName=rs.getString(4);
                iDocumentId=getDocumentId(iModuleTypeId,iContactId,sImageName);
                if(sImageName!=null && sImageName.trim().length()>0)
                {
                    LinkedHashMap clLinkedHashMap2 = new LinkedHashMap();
                    clLinkedHashMap2.put("name", sImageName);
                    clLinkedHashMap2.put("url", "/crmservices/rest/modules/v1.0/documents/" + CLUtilities.getAPITransId(iDocumentId, iModuleTypeId, 0));
                    clLinkedHashMap.put(sPictureFldName, clLinkedHashMap2);
                }
                clResultDTO.setCount(1);
                clResultDTO.setStatus(IMessageCodes.MSG_SUCCESS);
                ArrayList alCompanyDetails = new ArrayList();
                alCompanyDetails.add(clLinkedHashMap);
                clResultDTO.setRecords(alCompanyDetails);
            }
            rs.close();*/
        }
        catch (CLBusinessRuleException bre)
        {
            throw bre;
        }
        catch (Exception e)
        {
            logger.error("", e);
            throw new CLDaoException(e);
        }
        finally
        {
            releaseSqlBase();
        }
        return clResultDTO;
    }
    /*private int getDocumentId(int iModuleId, int iTransId, String sFileName) throws SQLException
    {
        int iDocumentId = 0;
        CLSqlBase clSqlBase = getSqlBase();
        Statement stmt = clSqlBase.getStatement();
        String sQuery=null;
        if(iModuleId==IConstants.IModule.ITypes.CALLS)
            sQuery="select iTransId from tCrm_CallAttach where iCallId="+iTransId+" and sFileName='" + sFileName + "'";
        else sQuery="select iDocumentId from vCrm_Documents where iTransId=" + iTransId + " and iModuleTypeId=" + iModuleId + " and sFileName='" + sFileName + "'";
        ResultSet rs = stmt.executeQuery(sQuery);
        if (rs.next())
            iDocumentId = rs.getInt(1);
        rs.close();
        return iDocumentId;
    }*/

    public CLStatusResponseDTO setMarkAsRead(int iModuleTypeId,long lTransId,int iUserId) throws CLDaoException
    {
        CLStatusResponseDTO clStatusResponseDTO=new CLStatusResponseDTO();
        try
        {
            int iTransId=CLUtilities.getAPITransIdToInt(lTransId, 0);
            CLNotification.getInstance().setModuleMessageAsRead(iTransId,iModuleTypeId,iUserId);
            clStatusResponseDTO.setStatus(IMessageCodes.MSG_SUCCESS);
        }
        catch (Exception e)
        {
            logger.info("", e);
            clStatusResponseDTO.setStatus(IMessageCodes.MSG_FAIL);
            CLErrorDTO clErrorDTO = new CLErrorDTO();
            clErrorDTO.setMessage(e.getMessage());
            ArrayList<CLErrorDTO> clErrorDTOs= new ArrayList<CLErrorDTO>();
            clErrorDTOs.add(clErrorDTO);
            clStatusResponseDTO.setErrors(clErrorDTOs);
            throw new CLDaoException(e);
        }
        return clStatusResponseDTO;
    }

    public CLResultDTO getQueryBasedMetaData(String sUserQuery)
    {
        CLResultDTO clResultDTO = new CLResultDTO();
        ArrayList<CLErrorDTO> clErrorDTOs = null;
        ArrayList<LinkedHashMap> clLinkedHashMaps=null;
        boolean bNodataFound=false;
        try
        {
            int iColCnt=1;
            initSqlBase();
            CLSqlBase clSqlBase = getSqlBase();
            Statement stmt= clSqlBase.getStatement();
            ResultSet rs = stmt.executeQuery(sUserQuery);
            ResultSetMetaData rsmd=rs.getMetaData();
            clLinkedHashMaps = new ArrayList<LinkedHashMap>();
            LinkedHashMap clLinkedHashMap =null;
            String sValue=null;
            while (rs.next())
            {
                clLinkedHashMap = new LinkedHashMap();
                for (int i = 1; i <= rsmd.getColumnCount(); i++)
                {
                    sValue=rs.getString(iColCnt++);
                    clLinkedHashMap.put(rsmd.getColumnName(i), (sValue!=null?sValue:""));
                }
                clLinkedHashMaps.add(clLinkedHashMap);
                iColCnt=1;
            }
            rs.close();
            if (clLinkedHashMaps.size() > 0 && clLinkedHashMaps.get(0).size() > 0)
            {
                clResultDTO.setCount(clLinkedHashMaps.size());
                clResultDTO.setStatus((short) 1);
                clResultDTO.setRecords(clLinkedHashMaps);
            }
            else
            {
                bNodataFound=true;
            }
            if(bNodataFound)
            {
                clResultDTO.setCount(0);
                clResultDTO.setStatus((short) 0);
                CLErrorDTO clErrorDTO = new CLErrorDTO();
                clErrorDTO.setErrorCode(IMessageCodes.MSG_NO_RECORDS_FOUND);
                clErrorDTOs = new ArrayList<CLErrorDTO>();
                clErrorDTOs.add(clErrorDTO);
                clResultDTO.setErrors(clErrorDTOs);
            }
        }
        catch (Exception e)
        {
            clResultDTO.setCount(0);
            clResultDTO.setStatus((short) 0);
            String sErrorMsg=e.getMessage() == null ? e.toString() : e.getMessage();
            clResultDTO.setMessage(sErrorMsg);
            CLErrorDTO clErrorDTO = new CLErrorDTO();
            clErrorDTO.setMessage(sErrorMsg);
            clErrorDTOs = new ArrayList<CLErrorDTO>();
            clErrorDTOs.add(clErrorDTO);
            clResultDTO.setErrors(clErrorDTOs);
            logger.error("", e);
            e.printStackTrace();
        }
        finally
        {
            releaseSqlBase();
        }
        return clResultDTO;
    }

    @Override
    public byte[] exportPrintLayout(String sModuleName, String sLayoutName, long lTransId, byte iFormatType) throws CLDaoException
    {
        CLResultDTO clResultDTO = new CLResultDTO();
        ArrayList<CLErrorDTO> clErrorDTOs = null;
        try
        {
            initSqlBase();
            CLSqlBase clSqlBase = getSqlBase();
            Statement stmt = clSqlBase.getStatement();
            int iTransId=0,iModuleId =0;
            ICompanyDTO clCompanyDTO = (CLApplicationContext.getInstance().getCompanyDTO() );
            iModuleId = clCompanyDTO.getModuleIdFromAPIName(sModuleName);
            if (iModuleId == 0)
                throw new CLBusinessRuleException(IMessageCodes.MSG_INVALID_MODULE_NAME);
            if (lTransId > 0)
                iTransId = CLUtilities.getAPITransIdToInt(lTransId, 0);

            ResultSet rs = stmt.executeQuery("select iLayoutId from cCrm_Layout where sLayoutName='" + sLayoutName + "' and iModuleId=" + iModuleId + " and iLayoutType=2");
            int iLayoutId=0;
            if(rs.next())
                iLayoutId=rs.getInt(1);
            rs.close();

            IPrintLayoutDao clPrintLayoutDao = (IPrintLayoutDao) CLServiceLocator.getDaoBean("IPrintLayoutDao");
            return (byte[]) clPrintLayoutDao.printPDF(iModuleId, iTransId, iLayoutId, true, null, 0, null)[0];
        }
        catch (Exception e)
        {
            clResultDTO.setCount(0);
            clResultDTO.setStatus((short) 0);
            String sErrorMsg=e.getMessage() == null ? e.toString() : e.getMessage();
            clResultDTO.setMessage(sErrorMsg);
            CLErrorDTO clErrorDTO = new CLErrorDTO();
            clErrorDTO.setMessage(sErrorMsg);
            clErrorDTOs = new ArrayList<CLErrorDTO>();
            clErrorDTOs.add(clErrorDTO);
            clResultDTO.setErrors(clErrorDTOs);
            logger.error("", e);
            e.printStackTrace();
        }
        finally
        {
            releaseSqlBase();
        }
        return null;//clResultDTO;
    }

    @Override
    public byte[] exportExternalReport(String sReportName,Object[] objRepFilter) throws CLDaoException
    {
        CLResultDTO clResultDTO = new CLResultDTO();
        ArrayList<CLErrorDTO> clErrorDTOs = null;
        try
        {
            initSqlBase();
            CLSqlBase clSqlBase = getSqlBase();
            Statement stmt = clSqlBase.getStatement();

            ResultSet rs = stmt.executeQuery("select iReportId from cCore_Reports_0 where sReportName='" + sReportName + "'");
            int iReportId=0;
            if(rs.next())
                iReportId=rs.getInt(1);
            rs.close();

            CLRepFilterDTO clRepFilterDTO = new CLRepFilterDTO();
            clRepFilterDTO.setReportId(iReportId);
            clRepFilterDTO.setMasterVariable((String[]) objRepFilter[0]);
            clRepFilterDTO.setMasterValues((String[]) objRepFilter[1]);

            IViewerDao clViewerDao = (IViewerDao) CLServiceLocator.getDaoBean("IViewerDao");
            Object[] objArrDetails=clViewerDao.getViewerDetails(iReportId, 2, clRepFilterDTO);
            if(objArrDetails!=null)
                return (byte[])objArrDetails[1];
            return new byte[]{0};
        }
        catch (Exception e)
        {
            clResultDTO.setCount(0);
            clResultDTO.setStatus((short) 0);
            String sErrorMsg=e.getMessage() == null ? e.toString() : e.getMessage();
            clResultDTO.setMessage(sErrorMsg);
            CLErrorDTO clErrorDTO = new CLErrorDTO();
            clErrorDTO.setMessage(sErrorMsg);
            clErrorDTOs = new ArrayList<CLErrorDTO>();
            clErrorDTOs.add(clErrorDTO);
            clResultDTO.setErrors(clErrorDTOs);
            logger.error("", e);
            e.printStackTrace();
        }
        finally
        {
            releaseSqlBase();
        }
        return null;//clResultDTO;
    }

    public CLResultDTO getScheduledReminders(boolean isShowAllReminders,boolean isFromNotifications) throws CLDaoException
    {
        CLResultDTO clResultDTO=new CLResultDTO();
        try
        {
            IReminderDao clReminderDao = (IReminderDao) CLServiceLocator.getDaoBean("IReminderDao");
            Object[] objResponse = clReminderDao.getScheduledReminders(isShowAllReminders, isFromNotifications);
            if( objResponse != null)
            {
                Object objReminders[] = (Object[])objResponse[1];
                if ( objReminders != null )
                {
                    ArrayList alReminders = null;
                    LinkedHashMap clLinkedHashMap = null ;
                    LinkedHashMap clGroupLinkedHashMap = new LinkedHashMap();
                    Object objChild [] = null;
                    for( int i = 0 ; i < objReminders.length ; i++ )
                    {
                        objChild = (Object[]) objReminders[i];
                        if (objChild != null)
                        {
                            CLReminderInfoDTO clReminderInfoDTO = null;
                            alReminders = new ArrayList();
                            for (int j = 0; j < objChild.length; j++)
                            {
                                if (objChild[j] != null)
                                {
                                    clReminderInfoDTO = (CLReminderInfoDTO) objChild[j];
                                    clLinkedHashMap = new LinkedHashMap();
                                    clLinkedHashMap.put("reminderId", clReminderInfoDTO.getReminderId());
                                    clLinkedHashMap.put("value1", clReminderInfoDTO.getValue1());
                                    clLinkedHashMap.put("value2", clReminderInfoDTO.getValue2());
                                    clLinkedHashMap.put("subject", clReminderInfoDTO.getSubject());
                                    clLinkedHashMap.put("dueIn", clReminderInfoDTO.getDueIn());
                                    clLinkedHashMap.put("eventDateTime", clReminderInfoDTO.getEventDateTime());
                                    clLinkedHashMap.put("moduleId", clReminderInfoDTO.getModuleId());
                                    clLinkedHashMap.put("relatedTo", clReminderInfoDTO.getRelatedTo());
                                    clLinkedHashMap.put("relatedToTitle", clReminderInfoDTO.getRelatedToTitle());
                                    clLinkedHashMap.put("relatedType", clReminderInfoDTO.getRelatedType());
                                    clLinkedHashMap.put("relatedTypeTitle", clReminderInfoDTO.getRelatedTypeTitle());
                                    alReminders.add(clLinkedHashMap);
                                }

                            }

                            clGroupLinkedHashMap.put(String.valueOf(((Object[]) objResponse[0])[i]), alReminders);

                        }
                    }
                    clResultDTO.setCount(1);
                    clResultDTO.setStatus(IMessageCodes.MSG_SUCCESS);
                    alReminders = new ArrayList();
                    alReminders.add(clGroupLinkedHashMap);
                    clResultDTO.setRecords(alReminders);
                }
            }
        }
        catch (Exception e)
        {
            logger.info("",e);
            clResultDTO.setCount(0);
            clResultDTO.setStatus(IMessageCodes.MSG_FAIL);
            CLErrorDTO clErrorDTO = new CLErrorDTO();
            clErrorDTO.setMessage(e.getMessage());
            ArrayList<CLErrorDTO> clErrorDTOs= new ArrayList<CLErrorDTO>();
            clErrorDTOs.add(clErrorDTO);
            clResultDTO.setErrors(clErrorDTOs);
            throw new CLDaoException(e);
        }
        return clResultDTO;
    }

    public CLResultDTO getTodayActivities(int iUserId,int iTaskStatus,int iAssinedTo) throws CLDaoException
    {
        CLResultDTO clResultDTO=new CLResultDTO();
        try
        {
            IActivityViewDao clActivityViewDao = (IActivityViewDao) CLServiceLocator.getDaoBean("IActivityViewDao");
            Object objResponse[] = clActivityViewDao.getTodayActivities(iUserId, iTaskStatus, iAssinedTo);
            if( objResponse != null)
            {

                Object objActivities[] = (Object[])objResponse[1];
                if ( objActivities != null )
                {
                    ArrayList alActivities = null;
                    LinkedHashMap clLinkedHashMap = null;
                    LinkedHashMap clGroupLinkedHashMap = new LinkedHashMap();
                    Object objChild [] = null;
                    Object objSubChild [] = null;
                    for( int i = 0 ; i < objActivities.length ; i++ )
                    {
                        objChild = (Object[]) objActivities[i];
                        if (objChild != null)
                        {
                            alActivities = new ArrayList();
                            for (int j = 0; j < objChild.length; j++)
                            {
                                if (objChild[j] != null)
                                {
                                    objSubChild = (Object[]) objChild[j];
                                    clLinkedHashMap = new LinkedHashMap();
                                    clLinkedHashMap.put("masterId", objSubChild[0]);
                                    clLinkedHashMap.put("activityType", objSubChild[0]);
                                    clLinkedHashMap.put("masterId", objSubChild[1]);
                                    clLinkedHashMap.put("subject", objSubChild[2]);
                                    clLinkedHashMap.put("relatedModuleName", objSubChild[3]);
                                    clLinkedHashMap.put("relatedModuleValue", objSubChild[4]);
                                    clLinkedHashMap.put("relatedToTypeName", objSubChild[5]);
                                    clLinkedHashMap.put("relatedToTypeValue", objSubChild[6]);
                                    clLinkedHashMap.put("relatedToName", objSubChild[7]);
                                    clLinkedHashMap.put("statusName", objSubChild[8]);
                                    clLinkedHashMap.put("statusValue", objSubChild[9]);
                                    clLinkedHashMap.put("assignedTo", objSubChild[10]);
                                    clLinkedHashMap.put("dueDate", objSubChild[11]);
                                    clLinkedHashMap.put("relatedToValue", objSubChild[12]);

                                    alActivities.add(clLinkedHashMap);
                                }

                            }
                            clGroupLinkedHashMap.put(String.valueOf(((Object[]) objResponse[0])[i]), alActivities);
                        }
                    }
                    clResultDTO.setCount(1);
                    clResultDTO.setStatus(IMessageCodes.MSG_SUCCESS);
                    alActivities = new ArrayList();
                    alActivities.add(clGroupLinkedHashMap);
                    clResultDTO.setRecords(alActivities);
                }
            }
        }
        catch (Exception e)
        {
            logger.info("",e);
            clResultDTO.setCount(0);
            clResultDTO.setStatus(IMessageCodes.MSG_FAIL);
            CLErrorDTO clErrorDTO = new CLErrorDTO();
            clErrorDTO.setMessage(e.getMessage());
            ArrayList<CLErrorDTO> clErrorDTOs= new ArrayList<CLErrorDTO>();
            clErrorDTOs.add(clErrorDTO);
            clResultDTO.setErrors(clErrorDTOs);
            throw new CLDaoException(e);
        }
        return clResultDTO;
    }

    public CLStatusResponseDTO doApprovalRecord(String  sModuleName,long lTransId,byte byLevel,int iUserId,int iStatus,String sDescription,long lMemberId) throws CLDaoException
    {
        int iTransId=0,iModuleId =0,iMemberId=0;
        ICompanyDTO clCompanyDTO = (CLApplicationContext.getInstance().getCompanyDTO() );
        iModuleId = clCompanyDTO.getModuleIdFromAPIName(sModuleName);
        if (iModuleId == 0)
            throw new CLBusinessRuleException(IMessageCodes.MSG_INVALID_MODULE_NAME);
        if (lTransId > 0)
            iTransId = CLUtilities.getAPITransIdToInt(lTransId, 0);
        if (lMemberId > 0)
            iMemberId = CLUtilities.getAPITransIdToInt(lMemberId, 0);

        return doApprovalRecord(iModuleId, iTransId, byLevel, iUserId, iStatus, sDescription, iMemberId);
    }

    public CLStatusResponseDTO doApprovalRecord(int iModuleId,int iTransId,byte byLevel,int iUserId,int iStatus,String sDescription,int iMemberId) throws CLDaoException
    {
        CLStatusResponseDTO clStatusResponseDTO=new CLStatusResponseDTO();
        try
        {
            CLModuleResolver clModuleResolver = (CLModuleResolver) CLServiceLocator.getAppBean("IModuleResolver");
            IModule clModule = clModuleResolver.getModuleDao(iModuleId);
            int arrStatus []=clModule.saveApproval(iModuleId, iTransId, byLevel, iUserId, iStatus, sDescription,0);
            int iReturnVal=arrStatus[1];
            clStatusResponseDTO.setStatus((short) iReturnVal);

        } catch (Exception e)
        {
            logger.info("", e);
            clStatusResponseDTO.setStatus(IMessageCodes.MSG_FAIL);
            CLErrorDTO clErrorDTO = new CLErrorDTO();
            clErrorDTO.setMessage(e.getMessage());
            ArrayList<CLErrorDTO> clErrorDTOs= new ArrayList<CLErrorDTO>();
            clErrorDTOs.add(clErrorDTO);
            clStatusResponseDTO.setErrors(clErrorDTOs);
            throw new CLDaoException(e);
        }
        return clStatusResponseDTO;
    }

    public CLStatusResponseDTO saveUnitConversion(CLAPIModuleDataDTO clapiModuleDataDTO) throws CLDaoException
    {
        CLStatusResponseDTO clStatusResponseDTO=null;
        try
        {
            ISearchLayout clSearchLayout=(ISearchLayout) CLServiceLocator.getDaoBean("ISearchLayoutDao");
            CLUnitConversionDTO clUnitConversionDTO=new CLUnitConversionDTO();
            ArrayList<CLErrorDTO> clErrorDTOs = null;
            LinkedHashMap clInvalidFldValues = new LinkedHashMap();
            Object[] objConversionData=getUnitConversionHeaderDTO(clapiModuleDataDTO,clUnitConversionDTO,clSearchLayout,clInvalidFldValues);
            clUnitConversionDTO= (CLUnitConversionDTO) objConversionData[0];
            ArrayList<CLKeyValueSI> alUnitFieldNames= (ArrayList<CLKeyValueSI>) objConversionData[1];
            ArrayList<CLKeyValueSI> alUnitBodyFldNames=null;
//            clInvalidFldValues.put(clapiFieldValueDTO.getFieldName(),clapiFieldValueDTO.getValue());

            if(clapiModuleDataDTO.getBody()!=null && clapiModuleDataDTO.getBody().size()>0)
            {
                objConversionData=getUnitConversionBodyDTO(clapiModuleDataDTO,clUnitConversionDTO,clSearchLayout,clInvalidFldValues);
                clUnitConversionDTO= (CLUnitConversionDTO) objConversionData[0];
                alUnitBodyFldNames= (ArrayList<CLKeyValueSI>) objConversionData[1];
            }
            CLFieldErrorDTO clFieldErrorDTO =null;
            CLErrorDTO clErrorDTO =null;
            Object[] objErrors = validateUnitConversion(clUnitConversionDTO,alUnitFieldNames,alUnitBodyFldNames);
            clFieldErrorDTO= (CLFieldErrorDTO) objErrors[0];
            clErrorDTO= (CLErrorDTO) objErrors[1];
            if (clInvalidFldValues.size() > 0)
            {
                if(clFieldErrorDTO==null)
                clFieldErrorDTO = new CLFieldErrorDTO();
                clFieldErrorDTO.setInvalidFieldValues(clInvalidFldValues);
//                if(clErrorDTOs==null)
//                    clErrorDTOs = new ArrayList<CLErrorDTO>();
//                clErrorDTOs.add(clFieldErrorDTO);
            }
            if(clFieldErrorDTO==null && clErrorDTO==null)
            {
                clStatusResponseDTO=new CLStatusResponseDTO();
                IUnitConversionDao clUnitConversionDao = (IUnitConversionDao) CLServiceLocator.getDaoBean("IUnitConversionDao");
                int iReturnVal = clUnitConversionDao.saveUnitConversion(clUnitConversionDTO);
                if (iReturnVal == 1) clStatusResponseDTO.setStatus((short) IMessageCodes.MSG_SUCCESS);
                else clStatusResponseDTO.setStatus((short) IMessageCodes.MSG_FAIL);
            }
            else
            {
                clErrorDTOs = new ArrayList<CLErrorDTO>();
                if(clFieldErrorDTO!=null)
                    clErrorDTOs.add(clFieldErrorDTO);
                if(clErrorDTO!=null)
                    clErrorDTOs.add(clErrorDTO);
                clStatusResponseDTO=new CLStatusResponseDTO();
                clStatusResponseDTO.setErrors(clErrorDTOs);
                return clStatusResponseDTO;
            }

        } catch (Exception e)
        {
            logger.info("", e);
            clStatusResponseDTO=new CLStatusResponseDTO();
            clStatusResponseDTO.setStatus(IMessageCodes.MSG_FAIL);
            CLErrorDTO clErrorDTO = new CLErrorDTO();
            clErrorDTO.setMessage(e.getMessage());
            ArrayList<CLErrorDTO> clErrorDTOs= new ArrayList<CLErrorDTO>();
            clErrorDTOs.add(clErrorDTO);
            clStatusResponseDTO.setErrors(clErrorDTOs);
            throw new CLDaoException(e);
        }
        return clStatusResponseDTO;
    }

    private Object[] setUnitConversionDTO(String sFldName, String sValue, CLUnitConversionDTO clUnitConversionDTO
            , CLUnitBodyDTO clUnitBodyDTO,ISearchLayout clSearchLayout,LinkedHashMap clInvalidFldValues)
    {
        int iValue = 0;
        String sField=null;

        if (sFldName.equalsIgnoreCase("xfactor") && clUnitBodyDTO!=null)
        {
            if(sValue.trim().length()>0 && CLUtilities.isNumber(sValue.trim()) && Integer.parseInt(sValue)>0)
                clUnitBodyDTO.setXFactor(sValue);
            else clInvalidFldValues.put(sFldName, sValue);
        }
        else if (sFldName.equalsIgnoreCase("roundoff") && clUnitBodyDTO!=null)
        {
            clUnitBodyDTO.setRoundOff(sValue);
        }
        else if (sFldName.equalsIgnoreCase("description") && clUnitBodyDTO!=null)
        {
            clUnitBodyDTO.setDescription(sValue);
        }
        else if (sFldName.contains("product__") && clUnitBodyDTO==null)
        {
            if (sFldName.contains("__"))
                sField = sFldName.substring(sFldName.lastIndexOf("_") + 1).toLowerCase();

            if (sField != null && sField.equalsIgnoreCase("name"))
            {
                iValue = clSearchLayout.getModuleSeqId(IConstants.IModule.ITypes.PRODUCTS, sValue, IConstants.IModule.ISearch.BY_NAME);
                if(iValue>0)
                    clUnitConversionDTO.setItem(iValue);
                else
                    clInvalidFldValues.put(sFldName,sValue);

            }
            else if (sField != null && sField.equalsIgnoreCase("code"))
            {
                iValue = clSearchLayout.getModuleSeqId(IConstants.IModule.ITypes.PRODUCTS, sValue, IConstants.IModule.ISearch.BY_CODE);
                if(iValue>0)
                    clUnitConversionDTO.setItem(iValue);
                else
                    clInvalidFldValues.put(sFldName,sValue);
            }
            else
            {
                if(sValue.trim().length()>0 && CLUtilities.isNumber(sValue.trim()) && Long.parseLong(sValue)>0)
                {
                    if(CLUtilities.getAPITransIdToInt(Long.parseLong(sValue), 0)>0)
                        clUnitConversionDTO.setItem(CLUtilities.getAPITransIdToInt(Long.parseLong(sValue), 0));
                    else clInvalidFldValues.put(sFldName, sValue);
                }
                else clInvalidFldValues.put(sFldName, sValue);
            }
        }
        else if (sFldName.contains("unit__") || sFldName.contains("baseUnit__"))
        {
            if (sFldName.contains("__"))
                sField = sFldName.substring(sFldName.lastIndexOf("_") + 1).toLowerCase();

            if (sField != null && sField.equalsIgnoreCase("name"))
            {
                iValue = clSearchLayout.getModuleSeqId(IConstants.IModule.ITypes.UNITS, sValue, IConstants.IModule.ISearch.BY_NAME);
                if(iValue>0)
                {
                    if(sFldName.contains("baseUnit__") && clUnitBodyDTO==null)
                        clUnitConversionDTO.setBaseUnit(iValue);
                    else clUnitBodyDTO.setUnitName(String.valueOf(iValue));
                }
                else
                    clInvalidFldValues.put(sFldName,sValue);
            }
            else if (sField != null && sField.equalsIgnoreCase("code"))
            {
                iValue = clSearchLayout.getModuleSeqId(IConstants.IModule.ITypes.UNITS, sValue, IConstants.IModule.ISearch.BY_CODE);
                if(iValue>0)
                {
                    if(sFldName.contains("baseUnit__") && clUnitBodyDTO==null)
                        clUnitConversionDTO.setBaseUnit(iValue);
                    else clUnitBodyDTO.setUnitName(String.valueOf(iValue));
                }
                else
                    clInvalidFldValues.put(sFldName, sValue);
            }
            else
            {
                if(sValue.trim().length()>0 && CLUtilities.isNumber(sValue.trim()) && Long.parseLong(sValue)>0)
                {
                    if(CLUtilities.getAPITransIdToInt(Long.parseLong(sValue), 0)>0)
                    {
                        if (sFldName.contains("baseUnit__") && clUnitBodyDTO == null) clUnitConversionDTO.setBaseUnit(CLUtilities.getAPITransIdToInt(Long.parseLong(sValue), 0));
                        else clUnitBodyDTO.setUnitName(String.valueOf(CLUtilities.getAPITransIdToInt(Long.parseLong(sValue), 0)));
                    }
                    else clInvalidFldValues.put(sFldName, sValue);
                }
                else clInvalidFldValues.put(sFldName, sValue);
            }
        }
        return new Object[]{clUnitBodyDTO==null?clUnitConversionDTO:clUnitBodyDTO};
    }

    private Object[] getUnitConversionHeaderDTO(CLAPIModuleDataDTO clapiModuleDataDTO, CLUnitConversionDTO clUnitConversionDTO, ISearchLayout clSearchLayout, LinkedHashMap clInvalidFldValues) throws SQLException
    {
        String sFldName = null;
        CLAPIFieldValueDTO clapiFieldValueDTO = null;
        ArrayList<CLAPIFieldValueDTO> clapiFieldValueDTOs = clapiModuleDataDTO.getFlds();
        ArrayList<CLKeyValueSI> alUnitFieldNames=new ArrayList<CLKeyValueSI>();
        int iFldIndex=0;
//        Object[] objConversionData=null;

        for (int i = 0; i < clapiFieldValueDTOs.size(); i++)
        {
            clapiFieldValueDTO = clapiFieldValueDTOs.get(i);
            sFldName = clapiFieldValueDTO.getFieldName();

            if (sFldName != null)
            {
                setUnitConversionDTO(sFldName, clapiFieldValueDTO.getValue(), clUnitConversionDTO,null, clSearchLayout,clInvalidFldValues);
                if (sFldName.contains("__"))
                    sFldName = sFldName.substring(0, sFldName.lastIndexOf("__"));
            }
            alUnitFieldNames.add(new CLKeyValueSI(sFldName, iFldIndex++));
        }
        return new Object[]{clUnitConversionDTO,alUnitFieldNames};
    }


    private Object[] getUnitConversionBodyDTO(CLAPIModuleDataDTO clapiModuleDataDTO, CLUnitConversionDTO clUnitConversionDTO, ISearchLayout clSearchLayout, LinkedHashMap clInvalidFldValues) throws SQLException
    {
        ArrayList alUnitBodyFldNames=new ArrayList();
        ArrayList alTotalUnitBodyFldNames=new ArrayList();
        ArrayList alApiBodyFlds = clapiModuleDataDTO.getBody();
        String sFldName = null;
        ArrayList<CLUnitBodyDTO> clUnitBodyDTOs=new ArrayList<CLUnitBodyDTO>();
        CLUnitBodyDTO clUnitBodyDTO=null;

        if (alApiBodyFlds != null && alApiBodyFlds.size() > 0)
        {
            String[] clFields = null, sRowValues = null;
            ArrayList<CLAPIBodyRowDTO> clRowValues = null;
            CLAPIBodyRowDTO clapiBodyRowDTO = null;
            CLAPIBodyValuesDTO clapiBodyValuesDTO=null;
            Object[] objConversionData=null;
            int iFldIndex=0;
            for (int i = 0; i < alApiBodyFlds.size(); i++)
            {
                clapiBodyValuesDTO = (CLAPIBodyValuesDTO) alApiBodyFlds.get(i);
                if (clapiBodyValuesDTO == null)
                    continue;

                clRowValues = clapiBodyValuesDTO.getRowValues();
                for (int j = 0; j < clRowValues.size(); j++)
                {
                    clUnitBodyDTO=new CLUnitBodyDTO();
                    clapiBodyRowDTO = clRowValues.get(j);
                    if (clapiBodyRowDTO.getFields() == null)
                        clFields = clapiBodyValuesDTO.getFields();
                    else clFields = clapiBodyRowDTO.getFields();
                    sRowValues = clapiBodyRowDTO.getValues();

                    for (int k = 0; k < clFields.length; k++)
                    {
                        sFldName = clFields[k];
                        if (sFldName != null)
                        {
                            objConversionData=setUnitConversionDTO(sFldName, sRowValues[k], null, clUnitBodyDTO, clSearchLayout,clInvalidFldValues);
                            if (sFldName.contains("__"))
                                sFldName = sFldName.substring(0, sFldName.lastIndexOf("__"));
                            alUnitBodyFldNames.add(new CLKeyValueSI(sFldName, iFldIndex++));
                        }
                    }
                    if(alUnitBodyFldNames.size()>0)
                        alTotalUnitBodyFldNames.add(alUnitBodyFldNames);
                    alUnitBodyFldNames=new ArrayList();
                    clUnitBodyDTOs.add((CLUnitBodyDTO) objConversionData[0]);
                }
            }
            clUnitConversionDTO.setUnitBodyDTO(clUnitBodyDTOs);
        }
        return new Object[]{clUnitConversionDTO,alTotalUnitBodyFldNames};
    }

    private Object[] validateUnitConversion(CLUnitConversionDTO clUnitConversionDTO, ArrayList<CLKeyValueSI> alUnitFieldNames, ArrayList alTotalUnitBodyFldNames)
    {
        ArrayList alUnitBodyFldNames=null;
        CLFieldErrorDTO clFieldErrorDTO=null;
        CLErrorDTO clErrorDTO=null;
        CLStatusResponseDTO clStatusResponseDTO=null;
        ArrayList<CLUnitBodyDTO> clUnitBodyDTOs=clUnitConversionDTO.getUnitBodyDTO();
        CLUnitBodyDTO clUnitBodyDTO=null;
        CLUnitBodyDTO clUnitBodyDTO2=null;
        int iBaseUnit=0,iUnitId=0;
        iBaseUnit=clUnitConversionDTO.getBaseUnit();

        String[] sMandatoryFlds =null;
        String[] sMandatoryBodyFlds =null;
        if(alTotalUnitBodyFldNames !=null)
        {
            sMandatoryBodyFlds = new String[2];
            sMandatoryBodyFlds[0] = "unit";
            sMandatoryBodyFlds[1] = "xfactor";

        }
        if(alUnitFieldNames!=null)
        {
            sMandatoryFlds = new String[1];
            sMandatoryFlds[0] = "baseUnit";
        }
        int iIndex=0;
        StringBuilder sbMandatoryFlds = null;
        if(sMandatoryFlds!=null)
            for (int i = 0; i < sMandatoryFlds.length; i++)
            {
                iIndex = CLUtilities.binarySearch(alUnitFieldNames.toArray(), sMandatoryFlds[i]);
                if (iIndex <= -1 )
                {
                    if (sbMandatoryFlds == null)
                        sbMandatoryFlds = new StringBuilder();
                    sbMandatoryFlds.append(sMandatoryFlds[i]).append(",");
                }
            }
        CLKeyValueSI clKeyValueSI=null;
        if(sMandatoryBodyFlds!=null)
            for (int j = 0; j < alTotalUnitBodyFldNames.size(); j++)
            {
                alUnitBodyFldNames= (ArrayList) alTotalUnitBodyFldNames.get(j);
                for (int i = 0; i < sMandatoryBodyFlds.length; i++)
                {
                    for (int k = 0; k < alUnitBodyFldNames.size(); k++)
                    {
                        clKeyValueSI = (CLKeyValueSI) alUnitBodyFldNames.get(k);
                        if (sMandatoryBodyFlds[i].equalsIgnoreCase(clKeyValueSI.getKey()))
                        {
                            iIndex = 1;
                            break;
                        } else iIndex = -1;
                    }
                    if (iIndex <= -1)
                    {
                        if (sbMandatoryFlds == null) sbMandatoryFlds = new StringBuilder();
                        sbMandatoryFlds.append(sMandatoryBodyFlds[i]).append(",");
                    }
                }
            }
        if (sbMandatoryFlds != null && sbMandatoryFlds.length() > 0)
        {
            sbMandatoryFlds = sbMandatoryFlds.deleteCharAt(sbMandatoryFlds.lastIndexOf(","));
            clStatusResponseDTO=new CLStatusResponseDTO();
            clStatusResponseDTO.setStatus(IMessageCodes.MSG_FAIL);
            clFieldErrorDTO = new CLFieldErrorDTO();
            clFieldErrorDTO.setMissingMandatoryFields(sbMandatoryFlds.toString().split(","));
        }

        if(clUnitBodyDTOs!=null && clUnitBodyDTOs.size()>0)
        {
            for (int i = 0; i < clUnitBodyDTOs.size(); i++)
            {
                clUnitBodyDTO = clUnitBodyDTOs.get(i);
                if(clUnitBodyDTO.getUnitName()!=null)
                {
                    iUnitId = Integer.parseInt(clUnitBodyDTO.getUnitName());
                    if (iBaseUnit == iUnitId)
                    {
                        if (clStatusResponseDTO == null) clStatusResponseDTO = new CLStatusResponseDTO();
                        clStatusResponseDTO.setStatus(IMessageCodes.MSG_FAIL);
                        if(clFieldErrorDTO ==null)
                            clFieldErrorDTO = new CLFieldErrorDTO();
                        clFieldErrorDTO.setMessage("Base unit and unit in line item should not be same.");
                        break;
                    }
                }
            }

            int iUnitId2=0;
            boolean isUnitAlreadyExist=false;
            for (int i = 0; i < clUnitBodyDTOs.size(); i++)
            {
                clUnitBodyDTO = clUnitBodyDTOs.get(i);
                if(clUnitBodyDTO.getUnitName()!=null && !isUnitAlreadyExist)
                {
                    iUnitId = Integer.parseInt(clUnitBodyDTO.getUnitName());

                    for (int j = 0; j < clUnitBodyDTOs.size(); j++)
                    {
                        if (i == j) continue;
                        clUnitBodyDTO2 = clUnitBodyDTOs.get(j);
                        if (clUnitBodyDTO2.getUnitName() != null)
                        {
                            iUnitId2 = Integer.parseInt(clUnitBodyDTO2.getUnitName());

                            if (iUnitId2 == iUnitId)
                            {
                                if (clStatusResponseDTO == null) clStatusResponseDTO = new CLStatusResponseDTO();
                                clStatusResponseDTO.setStatus(IMessageCodes.MSG_FAIL);
                                clErrorDTO = new CLErrorDTO();
                                clErrorDTO.setMessage("Same unit should not repeat in line items.");
                                isUnitAlreadyExist = true;
                                break;
                            }
                        }
                    }
                }
            }
        }
        else
        {
            if (clStatusResponseDTO == null) clStatusResponseDTO = new CLStatusResponseDTO();
            clStatusResponseDTO.setStatus(IMessageCodes.MSG_FAIL);
            if(clFieldErrorDTO==null)
                clFieldErrorDTO=new CLFieldErrorDTO();
            clFieldErrorDTO.setMessage("One line item is mandatory");
        }
        return new Object[]{clFieldErrorDTO,clErrorDTO};
    }

    public CLResultDTO getUnitConversionDetails() throws CLDaoException
    {
        CLResultDTO clResultDTO=new CLResultDTO();
        try
        {
            IUnitConversionDao clUnitConversionDao = (IUnitConversionDao) CLServiceLocator.getDaoBean("IUnitConversionDao");
            ArrayList clConversionList = clUnitConversionDao.getAPIUnitConversionInfo();
            CLUnitConversionDTO clUnitConversionDTO=null;
            ArrayList clUnitConversionList=new ArrayList();
            ArrayList<CLUnitBodyDTO> alUnitBodyList = null;
            CLUnitBodyDTO clUnitBodyDTO = null;
            if( clConversionList != null && clConversionList.size()>0)
            {
                ArrayList alUnits = null;
                LinkedHashMap clBodyLinkedHashMap = null;
                LinkedHashMap clUnitConversionLinkedHashMap = null;
                String[] sBaseUnitNames = null, sUnitNames = null, sProductNames = null;

                for (int j = 0; j < clConversionList.size(); j++)
                {
                    clUnitConversionDTO = (CLUnitConversionDTO) clConversionList.get(j);
                    alUnitBodyList = clUnitConversionDTO.getUnitBodyDTO();
                    if (alUnitBodyList != null)
                    {
                        alUnits = new ArrayList();
                        clUnitConversionLinkedHashMap = new LinkedHashMap();
                        for (int i = 0; i < alUnitBodyList.size(); i++)
                        {
                            sBaseUnitNames = clUnitConversionDTO.getBaseUnitName().split(",");
                            sProductNames = clUnitConversionDTO.getItemName().split(",");
                            if (sBaseUnitNames[0] != null && Long.parseLong(sBaseUnitNames[0]) > 0)
                            {
                                clUnitConversionLinkedHashMap.put("baseUnit__id", sBaseUnitNames[0]);
                                clUnitConversionLinkedHashMap.put("baseUnit__name", sBaseUnitNames[1]);
                                clUnitConversionLinkedHashMap.put("baseUnit__code", sBaseUnitNames[2]);
                            }
                            if (sProductNames[0] != null && Long.parseLong(sProductNames[0]) > 0)
                            {
                                clUnitConversionLinkedHashMap.put("product__id", sProductNames[0]);
                                clUnitConversionLinkedHashMap.put("product__name", sProductNames[1]);
                                clUnitConversionLinkedHashMap.put("product__code", sProductNames[2]);
                            }

                            clUnitBodyDTO = alUnitBodyList.get(i);
                            if (clUnitBodyDTO != null)
                            {
                                clBodyLinkedHashMap = new LinkedHashMap();
                                if (clUnitBodyDTO.getUnitName() != null)
                                {
                                    sUnitNames = clUnitBodyDTO.getUnitName().split(",");
                                    clBodyLinkedHashMap.put("unit__id", sUnitNames[0]);
                                    clBodyLinkedHashMap.put("unit__name", sUnitNames[1]);
                                    clBodyLinkedHashMap.put("unit__code", sUnitNames[2]);
                                }
                                clBodyLinkedHashMap.put("xfactor", clUnitBodyDTO.getXFactor());
                                clBodyLinkedHashMap.put("roundoff", clUnitBodyDTO.getRoundOff());
                                clBodyLinkedHashMap.put("description", clUnitBodyDTO.getDescription());
                                alUnits.add(clBodyLinkedHashMap);
                            }
                        }
                        clUnitConversionLinkedHashMap.put("lineItems", alUnits);
                    }
                    clUnitConversionList.add(clUnitConversionLinkedHashMap);
                }
                clResultDTO.setCount(clUnitConversionList.size());
                clResultDTO.setStatus(IMessageCodes.MSG_SUCCESS);
                clResultDTO.setRecords(clUnitConversionList);
            }
            else
            {
                clResultDTO.setCount(0);
                clResultDTO.setRecords(null);
                clResultDTO.setStatus((short) 0);
                CLErrorDTO clErrorDTO = new CLErrorDTO();
                clErrorDTO.setErrorCode(IMessageCodes.MSG_NO_RECORDS_FOUND);
                ArrayList<CLErrorDTO> clErrorDTOs = new ArrayList<CLErrorDTO>();
                clErrorDTOs.add(clErrorDTO);
                clResultDTO.setErrors(clErrorDTOs);
            }
        }
        catch (Exception e)
        {
            logger.info("",e);
            clResultDTO.setCount(0);
            clResultDTO.setStatus(IMessageCodes.MSG_FAIL);
            CLErrorDTO clErrorDTO = new CLErrorDTO();
            clErrorDTO.setMessage(e.getMessage());
            ArrayList<CLErrorDTO> clErrorDTOs= new ArrayList<CLErrorDTO>();
            clErrorDTOs.add(clErrorDTO);
            clResultDTO.setErrors(clErrorDTOs);
            throw new CLDaoException(e);
        }
        return clResultDTO;
    }

    public CLStatusResponseDTO sendSMS(CLAPISMSMessageDTO clapismsMessageDTO) throws CLDaoException {
        CLStatusResponseDTO clStatusResponseDTO = new CLStatusResponseDTO();
        try {
            initSqlBase();
            CLSqlBase clSqlBase = getSqlBase();
            ICompanyDTO clCompanyDTO =  CLApplicationContext.getInstance().getCompanyDTO();
            IBaseSessionDTO clBaseSessionDTO = CLApplicationContext.getInstance().getSessionDTO();
            ICommunicationDao clCommunicationDao = (ICommunicationDao) CLServiceLocator.getDaoBean("ICommunicationDao");
            clCommunicationDao.setMultiTrans(true);
            clCommunicationDao.setSqlBase(clSqlBase);
//            CLLoginJMSCallback clLoginJMSCallback = new CLLoginJMSCallback();
            CLAPIModuleSMSDTO clapiModuleSMSDTO = clapismsMessageDTO.getMessages().get(0);
            String sMobile = clapiModuleSMSDTO.getMobile();
            logger.error("inside sendSMS ==sMobile=="+sMobile);
            if (sMobile != null && sMobile.trim().length() > 0)
            {
                String[][] sStaticVals =null;
                if(clapiModuleSMSDTO.getPaymentLink()!=null)
                {
                    sStaticVals = new String[1][2];
                    sStaticVals[0][0] = "paymentLink";
                    sStaticVals[0][1] = clapiModuleSMSDTO.getPaymentLink();

                }
                else
                {
                    sStaticVals = new String[1][2];
                    sStaticVals[0][0] = "OTP";
                    sStaticVals[0][1] = clapiModuleSMSDTO.getOTP();
                }
                int iTemplateId=0,iTransId =0,iModuleId =0;
                if(clapiModuleSMSDTO.getTemplateId()<=0 && clapiModuleSMSDTO.getTemplateName()!=null)
                    iTemplateId=clCommunicationDao.getTemplateId(clapiModuleSMSDTO.getTemplateName());
                iTransId = CLUtilities.getAPITransIdToInt(clapiModuleSMSDTO.getTransId(), 0);
                logger.error("iTemplateId ==" + iTemplateId + "==iTransId==" + iTransId);
                if(clapiModuleSMSDTO.getModuleId()<=0 && clapiModuleSMSDTO.getModuleName()!=null)
                    iModuleId = clCompanyDTO.getModuleIdFromAPIName(clapiModuleSMSDTO.getModuleName());
                clCommunicationDao.sendSMS(iTemplateId,sMobile,clBaseSessionDTO.getLoginId(),
                        iTransId,iModuleId,clCompanyDTO.getCompanyCode(),false,0,sStaticVals);
                logger.error("aftr clCommunicationDao.sendSMS==");
                clStatusResponseDTO.setStatus(IMessageCodes.MSG_SUCCESS);
                clStatusResponseDTO.setMessage("Message sent successfully");
//                clCommunicationDao.sendSMS(clSmsMessage, IConstants.CommunicationErrors.Ignore.getCommunicationError());
            }
        }
        catch(CLBusinessRuleException bre)
        {
            clStatusResponseDTO.setStatus((short) 0);
            CLErrorDTO clErrorDTO = new CLErrorDTO();
            clErrorDTO.setErrorCode(bre.getMsgCode());
            clErrorDTO.setMessage(bre.getMessage());
            ArrayList<CLErrorDTO> clErrorDTOs = new ArrayList<CLErrorDTO>();
            clErrorDTOs.add(clErrorDTO);
            clStatusResponseDTO.setErrors(clErrorDTOs);
        }
        catch(Exception e)
        {
            clStatusResponseDTO.setStatus((short) 0);
            CLErrorDTO clErrorDTO = new CLErrorDTO();
            clErrorDTO.setMessage(e.getMessage());
            ArrayList<CLErrorDTO> clErrorDTOs = new ArrayList<CLErrorDTO>();
            clErrorDTOs.add(clErrorDTO);
            clStatusResponseDTO.setErrors(clErrorDTOs);
            logger.error("", e);
        }
        finally
        {
            releaseSqlBase();
        }
        return clStatusResponseDTO;
    }

    public CLResultDTO printPDF(String sModuleName, long lTransId, String sLayoutName, String sFileName) throws CLDaoException
    {
        Object[] objPrintData=null;
        CLResultDTO clResultDTO = new CLResultDTO();
        try
        {
            ICompanyDTO clCompanyDTO = (CLApplicationContext.getInstance().getCompanyDTO() );
//            byte byDateFormat=clCompanyDTO.getDateFormat();
//            byte byTimeFormat=clCompanyDTO.getTimeFormat();
            int iTransId = 0;
            int iModuleId = clCompanyDTO.getModuleIdFromAPIName(sModuleName);
            if (iModuleId == 0)
                throw new CLBusinessRuleException(IMessageCodes.MSG_INVALID_MODULE_NAME);

            if (lTransId > 0)
            {
                iTransId = CLUtilities.getAPITransIdToInt(lTransId, 0);

//                if (!isRecordExists(iTransId, iModuleId, clCompanyDTO))
//                    throw new CLBusinessRuleException(IMessageCodes.MSG_INVALID_MASTERID);
            }

            IPrintLayoutDao clPrintLayoutDao = (IPrintLayoutDao) CLServiceLocator.getDaoBean("IPrintLayoutDao");
            Object[] objPrintLayouts= clPrintLayoutDao.getLayoutsList(iModuleId);
//            Object[] objPrintLayouts= clPrintLayoutDao.getLayoutsList(iModuleId,new int[]{iTransId});
            CLKeyValueSII clKeyValueSII = null;
            int iLayoutType=0,iLayoutId=0;
            if(objPrintLayouts!=null && objPrintLayouts.length>0)
            {
                for (int i = 0; i < objPrintLayouts.length; i++)
                {
                    clKeyValueSII= (CLKeyValueSII) objPrintLayouts[i];
                    if(clKeyValueSII.getKey().equalsIgnoreCase(sLayoutName))
                    {
                        iLayoutType=clKeyValueSII.getValue2();
                        iLayoutId=clKeyValueSII.getId();
                        break;
                    }
                }
//                clKeyValueSII= (CLKeyValueSII) objPrintLayouts[0];
                if(sFileName==null || sFileName.trim().length()==0)
                    sFileName=sModuleName+"_"+iTransId+"_"+System.nanoTime();

                if(iLayoutType!=1)
                    objPrintData=clPrintLayoutDao.printPDF(iModuleId, iTransId, iLayoutId, true, sFileName, 0, null);
                else
                {
                    IViewerDao clViewerDao = (IViewerDao) CLServiceLocator.getDaoBean("IViewerDao");
                    int iOutput=2;//pdf
//                    Object[] objRepFilter =new Object[1];
                    CLRepFilterDTO clRepFilterDTO=new CLRepFilterDTO();
                    clRepFilterDTO.setMasterVariable(new String[]{"@iTransId"});
                    clRepFilterDTO.setMasterValues(new String[]{String.valueOf(iTransId)});
//                    objRepFilter[0]=clRepFilterDTO;
                    Object[] objPrintData2=clViewerDao.getViewerDetails(iLayoutId, iOutput, clRepFilterDTO);
                    objPrintData = new Object[2];
                    objPrintData[0]=objPrintData2[1];
                    objPrintData[1]=sFileName+".pdf";
                }
                clResultDTO.setCount(1);
                clResultDTO.setStatus(IMessageCodes.MSG_SUCCESS);
                ArrayList alData = new ArrayList();
                alData.add(objPrintData);
                clResultDTO.setRecords(alData);
            }
        }
        catch(Exception e)
        {
            logger.info("",e);
            clResultDTO.setCount(0);
            clResultDTO.setStatus(IMessageCodes.MSG_FAIL);
            CLErrorDTO clErrorDTO = new CLErrorDTO();
            clErrorDTO.setMessage(e.getMessage());
            ArrayList<CLErrorDTO> clErrorDTOs= new ArrayList<CLErrorDTO>();
            clErrorDTOs.add(clErrorDTO);
            clResultDTO.setErrors(clErrorDTOs);
            throw new CLDaoException(e);
        }
        finally
        {
            releaseSqlBase();
        }
        return clResultDTO;
    }

    @Override
    public CLResultDTO getLinkModuleValues(int iModuleTypeId, int iFieldId, int iLinkModuleTypeId, int iLinkTransId, boolean isEdit) throws CLDaoException
    {
        CLResultDTO clResultDTO = new CLResultDTO();
        try
        {
            IModule clModule = (IModule) CLServiceLocator.getAppBean("IModuleDao");
            Object[] objLinkModuleDets=clModule.getLinkModuleValues(iModuleTypeId, iFieldId, iLinkModuleTypeId, iLinkTransId, isEdit);
            ArrayList clLinkModule=new ArrayList();

            clLinkModule.add(getLinkMapFldsData(objLinkModuleDets));

            clResultDTO.setStatus(IMessageCodes.MSG_SUCCESS);
            clResultDTO.setRecords(clLinkModule);

        }
        catch (Exception e)
        {
            logger.info("",e);
            clResultDTO.setCount(0);
            clResultDTO.setStatus(IMessageCodes.MSG_FAIL);
            CLErrorDTO clErrorDTO = new CLErrorDTO();
            clErrorDTO.setMessage(e.getMessage());
            ArrayList<CLErrorDTO> clErrorDTOs= new ArrayList<CLErrorDTO>();
            clErrorDTOs.add(clErrorDTO);
            clResultDTO.setErrors(clErrorDTOs);
            throw new CLDaoException(e);
        }
        return clResultDTO;
    }

    private Object[] getLinkMapFldsData(Object[] objResponse)
    {
        ArrayList alHeaderData = null;
        ArrayList alBodyData = null;

        alHeaderData = new ArrayList();
        alBodyData = new ArrayList();
        CLBodyValuesDTO clBodyValuesDTO = null;
        CLFieldValueDTO clFieldValueDTO = null;
        Object objData[] = null;
        for (byte byIndex = 1; byIndex < objResponse.length; byIndex++)
        {
            objData = (Object[]) objResponse[byIndex];

            for (byte byIndex1 = 0; byIndex1 < objData.length; byIndex1++)
            {
                if (objData[byIndex1] instanceof CLBodyValuesDTO)
                {
                    clBodyValuesDTO = (CLBodyValuesDTO) objData[byIndex1];
                    alBodyData.add(new Object[]{clBodyValuesDTO.getFieldIds(), clBodyValuesDTO.getRowValuesStringArray(), clBodyValuesDTO.getLinkIds()});

                }
                else if (objData[byIndex1] instanceof CLFieldValueDTO)
                {
                    clFieldValueDTO = (CLFieldValueDTO) objData[byIndex1];
                    alHeaderData.add(new CLKeyValueSI(clFieldValueDTO.getFieldId(), clFieldValueDTO.getValue()));
                }
            }
        }
        return new Object[]{alHeaderData, alBodyData, objResponse[0]};
    }

    public CLResultDTO saveAttachments(String sModuleName,String sFieldName,int iTransId,String sFileNames) throws CLDaoException
    {
        CLResultDTO clResultDTO = new CLResultDTO();
        try
        {
            ICompanyDTO clCompanyDTO = (CLApplicationContext.getInstance().getCompanyDTO() );
            int iFieldId=0;
            int iModuleId = clCompanyDTO.getModuleIdFromAPIName(sModuleName);
            if (iModuleId == 0)
                throw new CLBusinessRuleException(IMessageCodes.MSG_INVALID_MODULE_NAME);
            String[] arrFileNames=null;
            if(sFileNames!=null)
                arrFileNames=sFileNames.split(",");

            initSqlBase();
            String sQuery="select iFieldId from vCrm_Fields where iTypeId="+iModuleId+"  and sFieldName='"+sFieldName+"' and iLanguageId=0";
            CLSqlBase clSqlBase = getSqlBase();
            Statement stmt = clSqlBase.getStatement();
            ResultSet rs = stmt.executeQuery(sQuery);
            if(rs.next())
                iFieldId=rs.getInt(1);
            rs.close();
            String sFileName=null;
            CLAttachmentDTO clAttachmentDTO = new CLAttachmentDTO();
            clAttachmentDTO.setModuleId(iModuleId);
            clAttachmentDTO.setTransId(iTransId);
            for (int i = 0; i < arrFileNames.length; i++)
            {
                sFileName=arrFileNames[i];
                CLFileAttachmentDTO clFileAttachmentDTO = new CLFileAttachmentDTO();
                clFileAttachmentDTO.setFieldId(iFieldId);
//                clFileAttachmentDTO.setName(iFieldId + "_"+sFileName);
                clFileAttachmentDTO.setName(sFileName);
                clAttachmentDTO.addFileAttachmentDTO(clFileAttachmentDTO);
            }

            CLProcessDocuments clProcessDocuments = new CLProcessDocuments(CLApplicationContext.getInstance().getSessionDTO(),clAttachmentDTO,null);
            clProcessDocuments.startService();

            clResultDTO.setCount(1);
            clResultDTO.setStatus(IMessageCodes.MSG_SUCCESS);
        }
        catch(Exception e)
        {
            logger.info("",e);
            clResultDTO.setCount(0);
            clResultDTO.setStatus(IMessageCodes.MSG_FAIL);
            CLErrorDTO clErrorDTO = new CLErrorDTO();
            clErrorDTO.setMessage(e.getMessage());
            ArrayList<CLErrorDTO> clErrorDTOs= new ArrayList<CLErrorDTO>();
            clErrorDTOs.add(clErrorDTO);
            clResultDTO.setErrors(clErrorDTOs);
            throw new CLDaoException(e);
        }
        finally
        {
            releaseSqlBase();
        }
        return clResultDTO;
    }

//    @Override
    private CLStatusResponseDTO sendPlainMail(CLAPIMailMessageDTO clapiMailMessageDTO)
    {
        CLStatusResponseDTO clStatusResponseDTO = new CLStatusResponseDTO();
        try
        {
            ICompanyDTO clCompanyDTO = (CLApplicationContext.getInstance().getCompanyDTO());
            CLSqlBase clSqlBase = getSqlBase();
            ICommunicationDao clCommunicationDao = (ICommunicationDao) CLServiceLocator.getDaoBean("ICommunicationDao");
            clCommunicationDao.setMultiTrans(true);
            clCommunicationDao.setSqlBase(clSqlBase);

            IEmailDao clEmailDao =(IEmailDao)CLServiceLocator.getDaoBean("IEmailDao");
            clEmailDao.setMultiTrans(true);
            clEmailDao.setSqlBase(clSqlBase);

            CLEmailSettingDTO clEmailSettingDTO= clEmailDao.getEmailSettingDetails();

            CLModuleMessageDTO clMailMessage = new CLModuleMessageDTO(clCompanyDTO.getCompanyId());
            clMailMessage.setCallback(this);
            clMailMessage.setEmailAddr(clapiMailMessageDTO.getTo());
            clMailMessage.setSubject(clapiMailMessageDTO.getSubject());
            clMailMessage.setMessage(clapiMailMessageDTO.getBody());
            clMailMessage.setCC(clapiMailMessageDTO.getCc());
            clMailMessage.setBCC(clapiMailMessageDTO.getBcc());
//            clMailMessage.setFailureUrl(clapiMailMessageDTO.getFailureUrl());
//            clMailMessage.setSuccessUrl(clapiMailMessageDTO.getSuccessUrl());
//            clMailMessage.setReferKey(clapiMailMessageDTO.getReferKey());

            clCommunicationDao.doTestMail(clEmailSettingDTO, clMailMessage);

            clStatusResponseDTO.setStatus((short) 1);
            clStatusResponseDTO.setMessage("Mail has been processed");
        }
        catch (CLBusinessRuleException bre)
        {
            clStatusResponseDTO.setStatus((short) 0);
            CLErrorDTO clErrorDTO = new CLErrorDTO();
            clErrorDTO.setErrorCode(bre.getMsgCode());
            clErrorDTO.setMessage(bre.getMessage());
            ArrayList<CLErrorDTO> clErrorDTOs = new ArrayList<CLErrorDTO>();
            clErrorDTOs.add(clErrorDTO);
            clStatusResponseDTO.setErrors(clErrorDTOs);
        }
        catch (Exception e)
        {
            clStatusResponseDTO.setStatus((short) 0);
            CLErrorDTO clErrorDTO = new CLErrorDTO();
            clErrorDTO.setMessage(e.getMessage());
            ArrayList<CLErrorDTO> clErrorDTOs = new ArrayList<CLErrorDTO>();
            clErrorDTOs.add(clErrorDTO);
            clStatusResponseDTO.setErrors(clErrorDTOs);
            logger.error("", e);
        }
        finally
        {
            releaseSqlBase();
        }
        return clStatusResponseDTO;
    }




/*
    private int getMemberId(int iModuleId,int iTransId) throws SQLException
    {
        int iMemberId=0;
        String sQuery="select iMemberId from vaCrm_TeleLeads where iTransId ="+iTransId;

        CLSqlBase clSqlBase = getSqlBase();
        Statement stmt = clSqlBase.getStatement();
        if(iModuleId==IConstants.IModule.ITypes.LEADS)
            sQuery="select iMemberId from vaCrm_Leads where iTransId ="+iTransId;
        ResultSet rs = stmt.executeQuery(sQuery);
        if(rs.next())
            iMemberId= rs.getInt(1);
        rs.close();
        return iMemberId;
    }

*/
}
